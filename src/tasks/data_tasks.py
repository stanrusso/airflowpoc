"""
Reusable data processing tasks for Airflow DAGs.
These tasks can be used across multiple pipelines.
"""
from airflow.decorators import task
import pandas as pd
from typing import Dict, Any, List, Optional


@task
def read_csv_data(
    source: str,
    separator: str = ",",
    columns: Optional[List[str]] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Read CSV data from a file path or URL.
    
    Args:
        source: File path or URL to the CSV file
        separator: Column separator (default: ',')
        columns: Optional list of columns to read
        
    Returns:
        Dictionary with 'data' key containing the DataFrame as dict
    """
    df = pd.read_csv(source, sep=separator, usecols=columns)
    return {
        "data": df.to_dict(),
        "row_count": len(df),
        "columns": list(df.columns),
    }


@task
def validate_data(
    data_dict: Dict[str, Any],
    required_columns: List[str],
    **kwargs
) -> Dict[str, Any]:
    """
    Validate that required columns exist and data is not empty.
    
    Args:
        data_dict: Dictionary containing 'data' key with DataFrame dict
        required_columns: List of column names that must be present
        
    Returns:
        Validation result with status and any missing columns
    """
    df = pd.DataFrame(data_dict["data"])
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    validation_result = {
        "is_valid": len(missing_columns) == 0 and len(df) > 0,
        "row_count": len(df),
        "missing_columns": missing_columns,
        "data": data_dict["data"],
    }
    
    if not validation_result["is_valid"]:
        raise ValueError(f"Data validation failed: {missing_columns}")
    
    return validation_result


@task
def transform_data(
    data_dict: Dict[str, Any],
    transformations: Dict[str, Any],
    **kwargs
) -> Dict[str, Any]:
    """
    Apply transformations to the data.
    
    Args:
        data_dict: Dictionary containing 'data' key with DataFrame dict
        transformations: Dict specifying transformations to apply
            - 'filter': dict with column and value to filter
            - 'groupby': dict with columns and aggregations
            - 'select': list of columns to keep
            
    Returns:
        Transformed data dictionary
    """
    df = pd.DataFrame(data_dict["data"])
    
    # Apply filter if specified
    if "filter" in transformations:
        filter_config = transformations["filter"]
        column = filter_config["column"]
        value = filter_config["value"]
        operator = filter_config.get("operator", "eq")
        
        if operator == "eq":
            df = df[df[column] == value]
        elif operator == "ne":
            df = df[df[column] != value]
        elif operator == "gt":
            df = df[df[column] > value]
        elif operator == "lt":
            df = df[df[column] < value]
    
    # Apply groupby if specified
    if "groupby" in transformations:
        groupby_config = transformations["groupby"]
        columns = groupby_config["columns"]
        agg = groupby_config.get("agg", "count")
        df = df.groupby(columns).agg(agg).reset_index()
    
    # Select specific columns if specified
    if "select" in transformations:
        df = df[transformations["select"]]
    
    return {
        "data": df.to_dict(),
        "row_count": len(df),
        "columns": list(df.columns),
    }


@task
def save_data(
    data_dict: Dict[str, Any],
    destination: str,
    format: str = "csv",
    **kwargs
) -> Dict[str, str]:
    """
    Save data to a file.
    
    Args:
        data_dict: Dictionary containing 'data' key with DataFrame dict
        destination: File path to save to
        format: Output format ('csv', 'parquet', 'json')
        
    Returns:
        Dictionary with save status and path
    """
    df = pd.DataFrame(data_dict["data"])
    
    if format == "csv":
        df.to_csv(destination, index=False)
    elif format == "parquet":
        df.to_parquet(destination, index=False)
    elif format == "json":
        df.to_json(destination, orient="records")
    else:
        raise ValueError(f"Unsupported format: {format}")
    
    return {
        "status": "success",
        "path": destination,
        "row_count": len(df),
    }


@task
def aggregate_results(
    *results: Dict[str, Any],
    **kwargs
) -> Dict[str, Any]:
    """
    Aggregate multiple task results into a single summary.
    
    Args:
        *results: Variable number of result dictionaries
        
    Returns:
        Aggregated summary
    """
    summary = {
        "total_tasks": len(results),
        "results": list(results),
    }
    return summary
