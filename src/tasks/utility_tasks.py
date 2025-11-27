"""
Reusable utility tasks for Airflow DAGs.
"""
from airflow.decorators import task
from airflow.providers.standard.operators.empty import EmptyOperator
from typing import Dict, Any
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)


@task
def log_start(dag_id: str, **kwargs) -> Dict[str, str]:
    """
    Log the start of a DAG execution.
    
    Args:
        dag_id: The DAG identifier
        
    Returns:
        Start information
    """
    start_time = datetime.now().isoformat()
    logger.info(f"Starting DAG: {dag_id} at {start_time}")
    
    return {
        "dag_id": dag_id,
        "start_time": start_time,
        "status": "started",
    }


@task
def log_end(
    dag_id: str,
    start_info: Dict[str, str],
    **kwargs
) -> Dict[str, Any]:
    """
    Log the end of a DAG execution with duration.
    
    Args:
        dag_id: The DAG identifier
        start_info: Start information from log_start task
        
    Returns:
        End information with duration
    """
    end_time = datetime.now()
    start_time = datetime.fromisoformat(start_info["start_time"])
    duration = (end_time - start_time).total_seconds()
    
    logger.info(f"Completed DAG: {dag_id} - Duration: {duration:.2f} seconds")
    
    return {
        "dag_id": dag_id,
        "start_time": start_info["start_time"],
        "end_time": end_time.isoformat(),
        "duration_seconds": duration,
        "status": "completed",
    }


@task.branch
def check_file_exists(
    file_path: str,
    exists_task_id: str,
    not_exists_task_id: str,
    **kwargs
) -> str:
    """
    Check if a file exists and branch accordingly.
    
    Args:
        file_path: Path to the file to check
        exists_task_id: Task ID to branch to if file exists
        not_exists_task_id: Task ID to branch to if file doesn't exist
        
    Returns:
        Task ID to execute next
    """
    if os.path.exists(file_path):
        logger.info(f"File exists: {file_path}")
        return exists_task_id
    else:
        logger.warning(f"File not found: {file_path}")
        return not_exists_task_id


@task
def wait_for_condition(
    condition_func: str,
    timeout_seconds: int = 300,
    poll_interval: int = 30,
    **kwargs
) -> Dict[str, Any]:
    """
    Wait for a condition to be true.
    
    Args:
        condition_func: String representation of condition to evaluate
        timeout_seconds: Maximum time to wait
        poll_interval: Time between checks
        
    Returns:
        Wait result
    """
    import time
    
    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        # In practice, implement your condition check here
        logger.info(f"Checking condition: {condition_func}")
        time.sleep(poll_interval)
        break  # Remove this in actual implementation
    
    return {
        "condition": condition_func,
        "waited_seconds": time.time() - start_time,
        "status": "completed",
    }


def create_start_end_tasks(dag_id: str):
    """
    Factory function to create start and end EmptyOperator tasks.
    
    Args:
        dag_id: The DAG identifier
        
    Returns:
        Tuple of (start_task, end_task)
    """
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    return start, end
