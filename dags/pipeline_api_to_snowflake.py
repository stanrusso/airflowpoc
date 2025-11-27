"""
Example pipeline: API to Snowflake ETL
Demonstrates the generic API-to-Snowflake pattern with reusable components.
"""
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator

# Import reusable components
from src.lib.api_client import (
    APISourceConfig,
    APIEndpointConfig,
    APIAuthConfig,
    AuthType,
    HttpMethod,
)
from src.lib.snowflake import SnowflakeTableConfig, WriteMode
from src.tasks.api_snowflake_tasks import (
    extract_from_api,
    load_to_snowflake,
    transform_api_data,
    validate_api_data,
    api_to_snowflake,
)
from src.settings import DAGConfig


# =============================================================================
# API Source Configuration
# =============================================================================

# Example: JSONPlaceholder API (free test API)
jsonplaceholder_config = APISourceConfig(
    name="jsonplaceholder",
    base_url="https://jsonplaceholder.typicode.com",
    auth=APIAuthConfig(auth_type=AuthType.NONE),
    endpoints={
        "users": APIEndpointConfig(
            name="users",
            path="/users",
            method=HttpMethod.GET,
            description="Fetch all users",
        ),
        "posts": APIEndpointConfig(
            name="posts",
            path="/posts",
            method=HttpMethod.GET,
            description="Fetch all posts",
            paginated=False,
        ),
        "comments": APIEndpointConfig(
            name="comments",
            path="/comments",
            method=HttpMethod.GET,
            description="Fetch all comments",
        ),
    }
)

# Example: GitHub API with Bearer token
github_config = APISourceConfig(
    name="github",
    base_url="https://api.github.com",
    auth=APIAuthConfig(
        auth_type=AuthType.BEARER_TOKEN,
        # Token would come from Airflow Variable or Secret
        # bearer_token=Variable.get("github_token")
    ),
    endpoints={
        "repos": APIEndpointConfig(
            name="repos",
            path="/user/repos",
            method=HttpMethod.GET,
            paginated=True,
            page_param="page",
            page_size_param="per_page",
            page_size=100,
        ),
    }
)


# =============================================================================
# Snowflake Table Configurations
# =============================================================================

users_table_config = SnowflakeTableConfig(
    database="RAW",
    schema="API_DATA",
    table="USERS",
    write_mode=WriteMode.REPLACE,
    auto_create=True,
    add_load_timestamp=True,
    add_source_column=True,
)

posts_table_config = SnowflakeTableConfig(
    database="RAW",
    schema="API_DATA",
    table="POSTS",
    write_mode=WriteMode.APPEND,
    auto_create=True,
    primary_keys=["id"],  # For potential upsert
    add_load_timestamp=True,
)


# =============================================================================
# DAG Definition
# =============================================================================

dag_config = DAGConfig(
    dag_id="api_to_snowflake_example",
    schedule="@daily",
    owner="data_team",
    description="Example: Extract data from APIs and load to Snowflake",
    tags=["api", "snowflake", "etl", "example"],
)


@dag(**dag_config.to_dag_kwargs())
def api_to_snowflake_example():
    """
    Example pipeline demonstrating API to Snowflake ETL pattern.
    
    This DAG:
    1. Extracts data from multiple API endpoints
    2. Validates and transforms the data
    3. Loads to Snowflake tables
    """
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    # -------------------------------------------------------------------------
    # Pattern 1: Extract -> Transform -> Load (separate tasks)
    # -------------------------------------------------------------------------
    
    # Extract users from API
    users_extracted = extract_from_api(
        source_config=jsonplaceholder_config.model_dump(),
        endpoint_name="users",
    )
    
    # Validate the extracted data
    users_validated = validate_api_data(
        data=users_extracted,
        required_columns=["id", "name", "email"],
        min_records=1,
    )
    
    # Transform: rename and select columns
    users_transformed = transform_api_data(
        data=users_validated,
        transformations={
            "rename_columns": {
                "id": "user_id",
                "name": "full_name",
            },
            "select_columns": ["user_id", "full_name", "email", "username"],
        }
    )
    
    # Load to Snowflake
    users_loaded = load_to_snowflake(
        data=users_transformed,
        table_config=users_table_config.model_dump(by_alias=True),
        snowflake_conn_id="snowflake_default",
    )
    
    # -------------------------------------------------------------------------
    # Pattern 2: Single task for simple ETL (no transformation)
    # -------------------------------------------------------------------------
    
    posts_loaded = api_to_snowflake(
        api_source_config=jsonplaceholder_config.model_dump(),
        endpoint_name="posts",
        table_config=posts_table_config.model_dump(by_alias=True),
        snowflake_conn_id="snowflake_default",
    )
    
    # -------------------------------------------------------------------------
    # Define task dependencies
    # -------------------------------------------------------------------------
    
    start >> [users_extracted, posts_loaded]
    users_extracted >> users_validated >> users_transformed >> users_loaded >> end
    posts_loaded >> end


# Instantiate the DAG
api_to_snowflake_example()
