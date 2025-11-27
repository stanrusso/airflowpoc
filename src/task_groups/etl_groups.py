"""
Reusable TaskGroups for common ETL patterns.
"""
from airflow.decorators import task, task_group
from airflow.utils.task_group import TaskGroup
from typing import Dict, Any, List, Optional
import pandas as pd


@task_group(group_id="extract")
def create_extract_group(
    sources: List[Dict[str, Any]],
):
    """
    Create a reusable extract task group.
    
    Args:
        sources: List of source configurations with 'name', 'type', and 'config'
        
    Usage:
        sources = [
            {"name": "users", "type": "csv", "config": {"path": "/data/users.csv"}},
            {"name": "orders", "type": "api", "config": {"url": "https://api.example.com/orders"}},
        ]
        extracted = create_extract_group(sources)
    """
    
    @task
    def extract_from_csv(source_config: Dict[str, Any]) -> Dict[str, Any]:
        path = source_config["config"]["path"]
        df = pd.read_csv(path)
        return {
            "name": source_config["name"],
            "data": df.to_dict(),
            "row_count": len(df),
        }
    
    @task
    def extract_from_api(source_config: Dict[str, Any]) -> Dict[str, Any]:
        # Implement API extraction
        return {
            "name": source_config["name"],
            "data": {},
            "row_count": 0,
        }
    
    results = []
    for source in sources:
        if source["type"] == "csv":
            result = extract_from_csv(source)
        elif source["type"] == "api":
            result = extract_from_api(source)
        results.append(result)
    
    return results


@task_group(group_id="transform")
def create_transform_group(
    data: Dict[str, Any],
    transformations: List[Dict[str, Any]],
):
    """
    Create a reusable transform task group.
    
    Args:
        data: Extracted data dictionary
        transformations: List of transformations to apply
        
    Usage:
        transformations = [
            {"type": "filter", "column": "status", "value": "active"},
            {"type": "rename", "mapping": {"old_name": "new_name"}},
        ]
        transformed = create_transform_group(data, transformations)
    """
    
    @task
    def apply_transformation(
        input_data: Dict[str, Any],
        transformation: Dict[str, Any]
    ) -> Dict[str, Any]:
        df = pd.DataFrame(input_data["data"])
        
        trans_type = transformation["type"]
        
        if trans_type == "filter":
            column = transformation["column"]
            value = transformation["value"]
            df = df[df[column] == value]
        elif trans_type == "rename":
            mapping = transformation["mapping"]
            df = df.rename(columns=mapping)
        elif trans_type == "drop_nulls":
            columns = transformation.get("columns")
            df = df.dropna(subset=columns)
        elif trans_type == "add_column":
            name = transformation["name"]
            value = transformation["value"]
            df[name] = value
        
        return {
            "data": df.to_dict(),
            "row_count": len(df),
            "transformation_applied": trans_type,
        }
    
    current_data = data
    for transformation in transformations:
        current_data = apply_transformation(current_data, transformation)
    
    return current_data


@task_group(group_id="load")
def create_load_group(
    data: Dict[str, Any],
    destination: Dict[str, Any],
):
    """
    Create a reusable load task group.
    
    Args:
        data: Transformed data dictionary
        destination: Destination configuration with 'type' and 'config'
        
    Usage:
        destination = {
            "type": "csv",
            "config": {"path": "/data/output.csv"}
        }
        load_result = create_load_group(data, destination)
    """
    
    @task
    def load_to_csv(
        input_data: Dict[str, Any],
        dest_config: Dict[str, Any]
    ) -> Dict[str, str]:
        df = pd.DataFrame(input_data["data"])
        path = dest_config["config"]["path"]
        df.to_csv(path, index=False)
        return {"status": "success", "path": path, "rows_written": len(df)}
    
    @task
    def load_to_database(
        input_data: Dict[str, Any],
        dest_config: Dict[str, Any]
    ) -> Dict[str, str]:
        # Implement database loading
        return {"status": "success", "table": dest_config["config"]["table"]}
    
    if destination["type"] == "csv":
        return load_to_csv(data, destination)
    elif destination["type"] == "database":
        return load_to_database(data, destination)


def create_etl_pipeline(
    sources: List[Dict[str, Any]],
    transformations: List[Dict[str, Any]],
    destination: Dict[str, Any],
) -> TaskGroup:
    """
    Factory function to create a complete ETL pipeline TaskGroup.
    
    Args:
        sources: List of source configurations
        transformations: List of transformations to apply
        destination: Destination configuration
        
    Returns:
        TaskGroup containing the complete ETL pipeline
        
    Usage:
        with DAG(...) as dag:
            etl = create_etl_pipeline(
                sources=[{"name": "data", "type": "csv", "config": {"path": "/data/input.csv"}}],
                transformations=[{"type": "filter", "column": "active", "value": True}],
                destination={"type": "csv", "config": {"path": "/data/output.csv"}}
            )
    """
    with TaskGroup(group_id="etl_pipeline") as etl_group:
        extracted = create_extract_group(sources)
        transformed = create_transform_group(extracted, transformations)
        loaded = create_load_group(transformed, destination)
    
    return etl_group
