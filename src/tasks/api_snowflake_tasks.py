"""
Reusable Airflow tasks for API extraction and Snowflake loading.
"""
import logging
from typing import Any, Dict, List, Optional

from airflow.decorators import task

logger = logging.getLogger(__name__)


# =============================================================================
# API Extraction Tasks
# =============================================================================

@task
def extract_from_api(
    source_config: Dict[str, Any],
    endpoint_name: str,
    extra_params: Optional[Dict[str, Any]] = None,
    extra_headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Extract data from an API endpoint.
    
    Args:
        source_config: Serialized APISourceConfig dictionary
        endpoint_name: Name of the endpoint to fetch
        extra_params: Additional query parameters
        extra_headers: Additional headers
        
    Returns:
        Dictionary with 'data' (list of records) and metadata
        
    Usage:
        source_config = APISourceConfig(...).model_dump()
        
        @dag(...)
        def my_dag():
            data = extract_from_api(
                source_config=source_config,
                endpoint_name="users"
            )
    """
    from src.lib.api_client import APISourceConfig, APIClient
    
    # Reconstruct config from dict
    config = APISourceConfig.model_validate(source_config)
    
    with APIClient(config) as client:
        records = client.fetch(endpoint_name, extra_params, extra_headers)
    
    return {
        "data": records,
        "source": config.name,
        "endpoint": endpoint_name,
        "record_count": len(records),
    }


@task
def extract_from_api_to_dataframe(
    source_config: Dict[str, Any],
    endpoint_name: str,
    extra_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Extract data from API and return as serialized DataFrame.
    
    Returns:
        Dictionary with serialized DataFrame and metadata
    """
    import pandas as pd
    from src.lib.api_client import APISourceConfig, APIClient
    
    config = APISourceConfig.model_validate(source_config)
    
    with APIClient(config) as client:
        df = client.fetch_to_dataframe(endpoint_name, extra_params)
    
    return {
        "data": df.to_dict(orient="records"),
        "columns": df.columns.tolist(),
        "source": config.name,
        "endpoint": endpoint_name,
        "record_count": len(df),
    }


@task
def extract_from_multiple_endpoints(
    source_config: Dict[str, Any],
    endpoints: List[str],
) -> Dict[str, Any]:
    """
    Extract data from multiple endpoints of the same API source.
    
    Returns:
        Dictionary with data from each endpoint
    """
    from src.lib.api_client import APISourceConfig, APIClient
    
    config = APISourceConfig.model_validate(source_config)
    results = {}
    
    with APIClient(config) as client:
        for endpoint_name in endpoints:
            try:
                records = client.fetch(endpoint_name)
                results[endpoint_name] = {
                    "data": records,
                    "record_count": len(records),
                    "status": "success",
                }
            except Exception as e:
                logger.error(f"Failed to fetch {endpoint_name}: {e}")
                results[endpoint_name] = {
                    "data": [],
                    "record_count": 0,
                    "status": "failed",
                    "error": str(e),
                }
    
    return {
        "source": config.name,
        "endpoints": results,
    }


# =============================================================================
# Snowflake Loading Tasks
# =============================================================================

@task
def load_to_snowflake(
    data: Dict[str, Any],
    table_config: Dict[str, Any],
    snowflake_conn_id: str = "snowflake_default",
    chunk_size: int = 10000,
) -> Dict[str, Any]:
    """
    Load extracted data to Snowflake.
    
    Args:
        data: Dictionary with 'data' key containing list of records
        table_config: Serialized SnowflakeTableConfig dictionary
        snowflake_conn_id: Airflow Snowflake connection ID
        chunk_size: Rows per chunk for loading
        
    Returns:
        Load statistics
        
    Usage:
        table_config = SnowflakeTableConfig(
            database="RAW",
            schema="API_DATA",
            table="USERS"
        ).model_dump()
        
        @dag(...)
        def my_dag():
            extracted = extract_from_api(...)
            loaded = load_to_snowflake(
                data=extracted,
                table_config=table_config
            )
    """
    import pandas as pd
    from src.lib.snowflake import SnowflakeTableConfig, load_dataframe_to_snowflake
    
    # Get records from extraction result
    records = data.get("data", data)
    if not records:
        logger.warning("No data to load")
        return {"success": True, "rows_loaded": 0, "message": "No data to load"}
    
    # Convert to DataFrame
    df = pd.DataFrame(records)
    
    # Reconstruct config
    config = SnowflakeTableConfig.model_validate(table_config)
    
    # Add source metadata if available
    if data.get("source") and config.add_source_column:
        config.source_value = data.get("source")
    
    # Load to Snowflake
    result = load_dataframe_to_snowflake(
        df=df,
        config=config,
        conn_id=snowflake_conn_id,
        chunk_size=chunk_size,
    )
    
    return result


@task
def upsert_to_snowflake(
    data: Dict[str, Any],
    table_config: Dict[str, Any],
    snowflake_conn_id: str = "snowflake_default",
) -> Dict[str, Any]:
    """
    Upsert (merge) data to Snowflake based on primary keys.
    
    Requires 'primary_keys' in table_config.
    """
    import pandas as pd
    from src.lib.snowflake import SnowflakeTableConfig, upsert_dataframe_to_snowflake
    
    records = data.get("data", data)
    if not records:
        return {"success": True, "rows_merged": 0, "message": "No data to upsert"}
    
    df = pd.DataFrame(records)
    config = SnowflakeTableConfig.model_validate(table_config)
    
    result = upsert_dataframe_to_snowflake(
        df=df,
        config=config,
        conn_id=snowflake_conn_id,
    )
    
    return result


@task
def load_dataframe_to_snowflake_task(
    df_dict: Dict[str, Any],
    table_config: Dict[str, Any],
    snowflake_conn_id: str = "snowflake_default",
) -> Dict[str, Any]:
    """
    Load a serialized DataFrame to Snowflake.
    
    Use this when passing DataFrames between tasks (serialized as dict).
    """
    import pandas as pd
    from src.lib.snowflake import SnowflakeTableConfig, load_dataframe_to_snowflake
    
    # Reconstruct DataFrame
    if "data" in df_dict:
        df = pd.DataFrame(df_dict["data"])
    else:
        df = pd.DataFrame(df_dict)
    
    config = SnowflakeTableConfig.model_validate(table_config)
    
    result = load_dataframe_to_snowflake(
        df=df,
        config=config,
        conn_id=snowflake_conn_id,
    )
    
    return result


# =============================================================================
# Combined ETL Tasks
# =============================================================================

@task
def api_to_snowflake(
    api_source_config: Dict[str, Any],
    endpoint_name: str,
    table_config: Dict[str, Any],
    snowflake_conn_id: str = "snowflake_default",
    extra_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Complete API to Snowflake ETL in a single task.
    
    Use for simple pipelines where intermediate processing isn't needed.
    
    Args:
        api_source_config: Serialized APISourceConfig
        endpoint_name: API endpoint to fetch
        table_config: Serialized SnowflakeTableConfig
        snowflake_conn_id: Airflow connection ID
        extra_params: Additional API parameters
        
    Returns:
        Combined extraction and load statistics
    """
    import pandas as pd
    from src.lib.api_client import APISourceConfig, APIClient
    from src.lib.snowflake import SnowflakeTableConfig, load_dataframe_to_snowflake
    
    # Extract
    api_config = APISourceConfig.model_validate(api_source_config)
    with APIClient(api_config) as client:
        df = client.fetch_to_dataframe(endpoint_name, extra_params)
    
    logger.info(f"Extracted {len(df)} records from {api_config.name}/{endpoint_name}")
    
    if df.empty:
        return {
            "success": True,
            "source": api_config.name,
            "endpoint": endpoint_name,
            "rows_extracted": 0,
            "rows_loaded": 0,
            "message": "No data extracted",
        }
    
    # Load
    sf_config = SnowflakeTableConfig.model_validate(table_config)
    sf_config.source_value = f"{api_config.name}/{endpoint_name}"
    
    load_result = load_dataframe_to_snowflake(
        df=df,
        config=sf_config,
        conn_id=snowflake_conn_id,
    )
    
    return {
        "success": load_result.get("success", False),
        "source": api_config.name,
        "endpoint": endpoint_name,
        "rows_extracted": len(df),
        "rows_loaded": load_result.get("rows_loaded", 0),
        "table": sf_config.full_table_name,
    }


# =============================================================================
# Data Transformation Tasks
# =============================================================================

@task
def transform_api_data(
    data: Dict[str, Any],
    transformations: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Apply transformations to extracted API data.
    
    Args:
        data: Extracted data dictionary
        transformations: Transformation configuration
            - 'rename_columns': Dict mapping old -> new column names
            - 'select_columns': List of columns to keep
            - 'filter': Dict with 'column', 'operator', 'value'
            - 'add_columns': Dict of new column name -> value
            
    Returns:
        Transformed data dictionary
    """
    import pandas as pd
    
    records = data.get("data", [])
    df = pd.DataFrame(records)
    
    # Rename columns
    if "rename_columns" in transformations:
        df = df.rename(columns=transformations["rename_columns"])
    
    # Select columns
    if "select_columns" in transformations:
        cols = [c for c in transformations["select_columns"] if c in df.columns]
        df = df[cols]
    
    # Filter rows
    if "filter" in transformations:
        f = transformations["filter"]
        col, op, val = f["column"], f["operator"], f["value"]
        if op == "eq":
            df = df[df[col] == val]
        elif op == "ne":
            df = df[df[col] != val]
        elif op == "gt":
            df = df[df[col] > val]
        elif op == "lt":
            df = df[df[col] < val]
        elif op == "in":
            df = df[df[col].isin(val)]
        elif op == "notnull":
            df = df[df[col].notna()]
    
    # Add columns
    if "add_columns" in transformations:
        for col_name, col_value in transformations["add_columns"].items():
            df[col_name] = col_value
    
    return {
        **data,
        "data": df.to_dict(orient="records"),
        "record_count": len(df),
        "columns": df.columns.tolist(),
    }


@task
def validate_api_data(
    data: Dict[str, Any],
    required_columns: List[str],
    min_records: int = 1,
) -> Dict[str, Any]:
    """
    Validate extracted API data before loading.
    
    Args:
        data: Extracted data dictionary
        required_columns: Columns that must be present
        min_records: Minimum number of records required
        
    Returns:
        Validated data with validation metadata
        
    Raises:
        ValueError: If validation fails
    """
    import pandas as pd
    
    records = data.get("data", [])
    
    if len(records) < min_records:
        raise ValueError(f"Expected at least {min_records} records, got {len(records)}")
    
    df = pd.DataFrame(records)
    missing_cols = [c for c in required_columns if c not in df.columns]
    
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    return {
        **data,
        "validation": {
            "passed": True,
            "record_count": len(records),
            "columns": df.columns.tolist(),
        }
    }
