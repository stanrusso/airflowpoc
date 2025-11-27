"""
Custom reusable operators for Airflow DAGs.
"""
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class DataQualityOperator(BaseOperator):
    """
    Custom operator to perform data quality checks.
    
    Usage:
        quality_check = DataQualityOperator(
            task_id="check_data_quality",
            checks=[
                {"type": "not_null", "column": "id"},
                {"type": "unique", "column": "email"},
                {"type": "range", "column": "age", "min": 0, "max": 120},
            ]
        )
    """
    
    template_fields = ["checks"]
    
    @apply_defaults
    def __init__(
        self,
        checks: List[Dict[str, Any]],
        fail_on_error: bool = True,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.checks = checks
        self.fail_on_error = fail_on_error
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute data quality checks."""
        results = []
        all_passed = True
        
        for check in self.checks:
            check_type = check.get("type")
            column = check.get("column")
            
            # Implement check logic here
            # This is a template - implement actual checks based on your data source
            logger.info(f"Running {check_type} check on column {column}")
            
            result = {
                "check_type": check_type,
                "column": column,
                "passed": True,  # Replace with actual check result
                "message": f"Check {check_type} passed for {column}",
            }
            results.append(result)
        
        summary = {
            "total_checks": len(results),
            "passed": sum(1 for r in results if r["passed"]),
            "failed": sum(1 for r in results if not r["passed"]),
            "all_passed": all_passed,
            "results": results,
        }
        
        if not all_passed and self.fail_on_error:
            raise ValueError(f"Data quality checks failed: {summary}")
        
        return summary


class SlackNotificationOperator(BaseOperator):
    """
    Custom operator to send Slack notifications.
    
    Usage:
        notify = SlackNotificationOperator(
            task_id="notify_slack",
            channel="#data-alerts",
            message="Pipeline completed successfully!",
        )
    """
    
    template_fields = ["message", "channel"]
    
    @apply_defaults
    def __init__(
        self,
        channel: str,
        message: str,
        webhook_conn_id: str = "slack_webhook",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.channel = channel
        self.message = message
        self.webhook_conn_id = webhook_conn_id
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, str]:
        """Send Slack notification."""
        logger.info(f"Sending message to {self.channel}: {self.message}")
        
        # Implement actual Slack webhook call here
        # from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
        # hook = SlackWebhookHook(slack_webhook_conn_id=self.webhook_conn_id)
        # hook.send(text=self.message, channel=self.channel)
        
        return {
            "status": "sent",
            "channel": self.channel,
            "message": self.message,
        }
