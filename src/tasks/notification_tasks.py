"""
Reusable notification tasks for Airflow DAGs.
"""
from airflow.decorators import task
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


@task
def send_success_notification(
    dag_id: str,
    message: Optional[str] = None,
    **kwargs
) -> Dict[str, str]:
    """
    Send a success notification.
    
    Args:
        dag_id: The DAG identifier
        message: Optional custom message
        
    Returns:
        Notification status
    """
    notification_message = message or f"DAG {dag_id} completed successfully!"
    logger.info(f"SUCCESS: {notification_message}")
    
    # Add your notification logic here (email, Slack, Teams, etc.)
    # Example: send_slack_message(notification_message)
    
    return {
        "status": "sent",
        "type": "success",
        "message": notification_message,
    }


@task
def send_failure_notification(
    dag_id: str,
    error_message: str,
    **kwargs
) -> Dict[str, str]:
    """
    Send a failure notification.
    
    Args:
        dag_id: The DAG identifier
        error_message: The error message to include
        
    Returns:
        Notification status
    """
    notification_message = f"DAG {dag_id} failed: {error_message}"
    logger.error(f"FAILURE: {notification_message}")
    
    # Add your notification logic here
    
    return {
        "status": "sent",
        "type": "failure",
        "message": notification_message,
    }


@task.branch
def check_status_and_notify(
    status: str,
    success_task_id: str = "notify_success",
    failure_task_id: str = "notify_failure",
    **kwargs
) -> str:
    """
    Branch based on status to route to appropriate notification.
    
    Args:
        status: The status to check ('success' or 'failure')
        success_task_id: Task ID to branch to on success
        failure_task_id: Task ID to branch to on failure
        
    Returns:
        Task ID to execute next
    """
    if status == "success":
        return success_task_id
    return failure_task_id
