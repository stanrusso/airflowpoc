"""
Snowflake utilities and helper functions for DataFrame operations.
Uses Airflow's Snowflake provider for connections and operations.
"""
import logging
from typing import Any, Dict, List, Optional, Literal
from enum import Enum

import pandas as pd
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================

class WriteMode(str, Enum):
    """Write modes for Snowflake operations."""
    APPEND = "append"
    REPLACE = "replace"
    TRUNCATE = "truncate"
    UPSERT = "upsert"


class SnowflakeDataType(str, Enum):
    """Common Snowflake data types for schema mapping."""
    VARCHAR = "VARCHAR"
    NUMBER = "NUMBER"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMP_NTZ = "TIMESTAMP_NTZ"
    TIMESTAMP_TZ = "TIMESTAMP_TZ"
    VARIANT = "VARIANT"
    OBJECT = "OBJECT"
    ARRAY = "ARRAY"


# =============================================================================
# Pydantic Settings for Snowflake Connection
# =============================================================================

class SnowflakeSettings(BaseSettings):
    """
    Snowflake connection settings loaded from environment variables.
    
    Environment variables (prefix: SNOWFLAKE_):
        SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
        SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE, etc.
    """
    model_config = SettingsConfigDict(
        env_prefix="SNOWFLAKE_",
        env_file=".env",
        extra="ignore",
    )
    
    # Connection
    account: str = ""
    user: str = ""
    password: str = ""
    
    # Default database settings
    database: str = "RAW"
    schema_name: str = Field(default="PUBLIC", alias="schema")
    warehouse: str = "COMPUTE_WH"
    role: Optional[str] = None
    
    # Airflow connection ID (alternative to direct credentials)
    airflow_conn_id: str = "snowflake_default"
    
    # Default write settings
    default_write_mode: WriteMode = WriteMode.APPEND
    default_chunk_size: int = 10000
    auto_create_table: bool = True
    
    # Stage settings
    default_stage: str = "@~"
    use_internal_stage: bool = True


class SnowflakeTableConfig(BaseModel):
    """Configuration for a Snowflake table destination."""
    database: str
    schema_name: str = Field(alias="schema")
    table: str
    
    # Write options
    write_mode: WriteMode = WriteMode.APPEND
    primary_keys: List[str] = Field(default_factory=list)  # For upsert
    
    # Schema options
    auto_create: bool = True
    column_mapping: Dict[str, str] = Field(default_factory=dict)  # source_col -> target_col
    column_types: Dict[str, SnowflakeDataType] = Field(default_factory=dict)
    
    # Data quality
    drop_duplicates: bool = False
    dedupe_columns: List[str] = Field(default_factory=list)
    
    # Metadata
    add_load_timestamp: bool = True
    load_timestamp_column: str = "_loaded_at"
    add_source_column: bool = False
    source_column_name: str = "_source"
    source_value: Optional[str] = None
    
    model_config = {"populate_by_name": True}
    
    @property
    def full_table_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.database}.{self.schema_name}.{self.table}"
    
    def get_target_columns(self, source_columns: List[str]) -> List[str]:
        """Map source columns to target columns."""
        return [self.column_mapping.get(col, col) for col in source_columns]


# =============================================================================
# DataFrame Preparation Utilities
# =============================================================================

def prepare_dataframe_for_snowflake(
    df: pd.DataFrame,
    config: SnowflakeTableConfig,
) -> pd.DataFrame:
    """
    Prepare a DataFrame for loading into Snowflake.
    
    Args:
        df: Source DataFrame
        config: Table configuration
        
    Returns:
        Prepared DataFrame ready for Snowflake loading
    """
    df = df.copy()
    
    # Drop duplicates if configured
    if config.drop_duplicates:
        dedupe_cols = config.dedupe_columns or df.columns.tolist()
        df = df.drop_duplicates(subset=dedupe_cols)
        logger.info(f"Dropped duplicates, rows remaining: {len(df)}")
    
    # Rename columns if mapping provided
    if config.column_mapping:
        df = df.rename(columns=config.column_mapping)
        logger.info(f"Renamed columns: {config.column_mapping}")
    
    # Add load timestamp
    if config.add_load_timestamp:
        from datetime import datetime, timezone
        df[config.load_timestamp_column] = datetime.now(timezone.utc)
    
    # Add source column
    if config.add_source_column and config.source_value:
        df[config.source_column_name] = config.source_value
    
    # Convert column names to uppercase (Snowflake convention)
    df.columns = [col.upper() for col in df.columns]
    
    # Handle data types
    df = _convert_types_for_snowflake(df)
    
    return df


def _convert_types_for_snowflake(df: pd.DataFrame) -> pd.DataFrame:
    """Convert pandas types to Snowflake-compatible types."""
    for col in df.columns:
        # Convert datetime columns
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        # Convert boolean to uppercase string (Snowflake preference)
        elif pd.api.types.is_bool_dtype(df[col]):
            df[col] = df[col].map({True: "TRUE", False: "FALSE", None: None})
        # Handle NaN values
        elif pd.api.types.is_float_dtype(df[col]):
            df[col] = df[col].where(pd.notna(df[col]), None)
    
    return df


def generate_create_table_sql(
    df: pd.DataFrame,
    config: SnowflakeTableConfig,
) -> str:
    """
    Generate CREATE TABLE SQL statement based on DataFrame schema.
    
    Args:
        df: Source DataFrame (after preparation)
        config: Table configuration
        
    Returns:
        CREATE TABLE SQL statement
    """
    columns = []
    
    for col in df.columns:
        col_upper = col.upper()
        
        # Use explicit type if provided
        if col_upper in config.column_types:
            sf_type = config.column_types[col_upper].value
        else:
            # Infer type from pandas dtype
            sf_type = _pandas_to_snowflake_type(df[col].dtype)
        
        columns.append(f"    {col_upper} {sf_type}")
    
    columns_sql = ",\n".join(columns)
    
    sql = f"""
CREATE TABLE IF NOT EXISTS {config.full_table_name} (
{columns_sql}
)
""".strip()
    
    return sql


def _pandas_to_snowflake_type(dtype) -> str:
    """Map pandas dtype to Snowflake data type."""
    dtype_str = str(dtype)
    
    if "int" in dtype_str:
        return "NUMBER"
    elif "float" in dtype_str:
        return "FLOAT"
    elif "bool" in dtype_str:
        return "BOOLEAN"
    elif "datetime" in dtype_str:
        return "TIMESTAMP_NTZ"
    elif "object" in dtype_str:
        return "VARCHAR(16777216)"  # Max VARCHAR
    else:
        return "VARCHAR(16777216)"


def generate_merge_sql(
    config: SnowflakeTableConfig,
    columns: List[str],
    stage_table: str,
) -> str:
    """
    Generate MERGE SQL for upsert operations.
    
    Args:
        config: Table configuration with primary keys
        columns: List of column names
        stage_table: Name of staging table
        
    Returns:
        MERGE SQL statement
    """
    if not config.primary_keys:
        raise ValueError("primary_keys required for upsert operations")
    
    # Build ON clause from primary keys
    on_conditions = " AND ".join([
        f"target.{pk.upper()} = source.{pk.upper()}"
        for pk in config.primary_keys
    ])
    
    # Build UPDATE SET clause (exclude primary keys)
    update_cols = [c for c in columns if c.upper() not in [pk.upper() for pk in config.primary_keys]]
    update_set = ", ".join([f"{c.upper()} = source.{c.upper()}" for c in update_cols])
    
    # Build INSERT clause
    insert_cols = ", ".join([c.upper() for c in columns])
    insert_vals = ", ".join([f"source.{c.upper()}" for c in columns])
    
    sql = f"""
MERGE INTO {config.full_table_name} AS target
USING {stage_table} AS source
ON {on_conditions}
WHEN MATCHED THEN UPDATE SET {update_set}
WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
""".strip()
    
    return sql


# =============================================================================
# Snowflake Operations using Airflow Hook
# =============================================================================

def get_snowflake_hook(conn_id: str = "snowflake_default"):
    """Get Snowflake hook from Airflow connection."""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    return SnowflakeHook(snowflake_conn_id=conn_id)


def execute_snowflake_query(
    sql: str,
    conn_id: str = "snowflake_default",
    parameters: Optional[Dict[str, Any]] = None,
) -> List[tuple]:
    """Execute a SQL query and return results."""
    hook = get_snowflake_hook(conn_id)
    return hook.get_records(sql, parameters=parameters)


def load_dataframe_to_snowflake(
    df: pd.DataFrame,
    config: SnowflakeTableConfig,
    conn_id: str = "snowflake_default",
    chunk_size: int = 10000,
) -> Dict[str, Any]:
    """
    Load a DataFrame to Snowflake using the write_pandas method.
    
    Args:
        df: DataFrame to load
        config: Table configuration
        conn_id: Airflow Snowflake connection ID
        chunk_size: Number of rows per chunk
        
    Returns:
        Dictionary with load statistics
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    
    # Prepare the DataFrame
    df_prepared = prepare_dataframe_for_snowflake(df, config)
    
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    conn = hook.get_conn()
    
    try:
        # Set context
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {config.database}")
        cursor.execute(f"USE SCHEMA {config.schema_name}")
        
        # Handle different write modes
        if config.write_mode == WriteMode.REPLACE:
            cursor.execute(f"DROP TABLE IF EXISTS {config.table}")
            logger.info(f"Dropped existing table: {config.table}")
        elif config.write_mode == WriteMode.TRUNCATE:
            cursor.execute(f"TRUNCATE TABLE IF EXISTS {config.table}")
            logger.info(f"Truncated table: {config.table}")
        
        # Create table if needed
        if config.auto_create:
            create_sql = generate_create_table_sql(df_prepared, config)
            cursor.execute(create_sql)
            logger.info(f"Ensured table exists: {config.full_table_name}")
        
        # Load data
        from snowflake.connector.pandas_tools import write_pandas
        
        success, num_chunks, num_rows, _ = write_pandas(
            conn=conn,
            df=df_prepared,
            table_name=config.table,
            database=config.database,
            schema=config.schema_name,
            chunk_size=chunk_size,
            auto_create_table=False,  # We handle this above
            overwrite=False,  # We handle modes above
        )
        
        result = {
            "success": success,
            "table": config.full_table_name,
            "rows_loaded": num_rows,
            "chunks": num_chunks,
            "write_mode": config.write_mode.value,
        }
        
        logger.info(f"Loaded {num_rows} rows to {config.full_table_name}")
        return result
        
    finally:
        conn.close()


def upsert_dataframe_to_snowflake(
    df: pd.DataFrame,
    config: SnowflakeTableConfig,
    conn_id: str = "snowflake_default",
) -> Dict[str, Any]:
    """
    Upsert (merge) a DataFrame to Snowflake.
    
    Requires primary_keys to be set in config.
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    import uuid
    
    if not config.primary_keys:
        raise ValueError("primary_keys must be set for upsert operations")
    
    # Prepare DataFrame
    df_prepared = prepare_dataframe_for_snowflake(df, config)
    
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    conn = hook.get_conn()
    
    # Create temporary staging table
    stage_table = f"{config.table}_STAGE_{uuid.uuid4().hex[:8].upper()}"
    stage_config = SnowflakeTableConfig(
        database=config.database,
        schema=config.schema_name,
        table=stage_table,
        write_mode=WriteMode.REPLACE,
        auto_create=True,
    )
    
    try:
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {config.database}")
        cursor.execute(f"USE SCHEMA {config.schema_name}")
        
        # Load to staging table
        from snowflake.connector.pandas_tools import write_pandas
        
        write_pandas(
            conn=conn,
            df=df_prepared,
            table_name=stage_table,
            database=config.database,
            schema=config.schema_name,
            auto_create_table=True,
            overwrite=True,
        )
        logger.info(f"Loaded {len(df_prepared)} rows to staging table: {stage_table}")
        
        # Ensure target table exists
        if config.auto_create:
            create_sql = generate_create_table_sql(df_prepared, config)
            cursor.execute(create_sql)
        
        # Execute merge
        merge_sql = generate_merge_sql(
            config,
            df_prepared.columns.tolist(),
            f"{config.database}.{config.schema_name}.{stage_table}",
        )
        cursor.execute(merge_sql)
        
        # Get merge statistics
        result = cursor.fetchone()
        rows_inserted = result[0] if result else 0
        
        return {
            "success": True,
            "table": config.full_table_name,
            "rows_merged": len(df_prepared),
            "write_mode": "upsert",
            "primary_keys": config.primary_keys,
        }
        
    finally:
        # Cleanup staging table
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {stage_table}")
        except Exception as e:
            logger.warning(f"Failed to drop staging table: {e}")
        conn.close()
