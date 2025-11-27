"""
Centralized DAG configuration settings using Pydantic.
"""
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


# =============================================================================
# Environment Settings (loaded from environment variables)
# =============================================================================


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter=".",
        env_nested_max_split=2,
        extra="ignore",
    )

    # default dag args
    # notification settings
    # data source settings

    dag_args: DagArgs
    notification: NotificationSettings
    data_sources: DataSourceSettings
    
    # Default DAG settings
    default_owner: str = "airflow"
    default_retries: int = 1
    default_retry_delay_minutes: int = 5
    default_start_date: datetime = datetime(2024, 1, 1)
    
    # Email settings
    email_on_failure: bool = False
    email_on_retry: bool = False
    alert_emails: List[str] = Field(default_factory=list)
    
    # Execution settings
    default_max_active_runs: int = 1
    default_catchup: bool = False
    depends_on_past: bool = False
    
    # Connection settings
    default_pool: str = "default_pool"
    default_queue: str = "default"
    
    @field_validator("alert_emails", mode="before")
    @classmethod
    def parse_emails(cls, v):
        """Parse comma-separated email string into list."""
        if isinstance(v, str):
            return [e.strip() for e in v.split(",") if e.strip()]
        return v


class DataSourceSettings(BaseSettings):
    """
    Data source connection settings loaded from environment variables.
    
    Usage:
        sources = DataSourceSettings()
        print(sources.database_conn_id)
    """
    model_config = SettingsConfigDict(
        env_prefix="DATA_",
        env_file=".env",
        extra="ignore",
    )
    
    # Database connections
    database_conn_id: str = "postgres_default"
    warehouse_conn_id: str = "warehouse_default"
    
    # Cloud storage
    s3_conn_id: str = "aws_default"
    s3_bucket: str = "data-lake"
    gcs_conn_id: str = "google_cloud_default"
    gcs_bucket: str = "data-lake"
    azure_conn_id: str = "azure_default"
    azure_container: str = "data-lake"
    
    # API connections
    api_conn_id: str = "http_default"
    api_timeout_seconds: int = 30


class NotificationSettings(BaseSettings):
    """
    Notification settings loaded from environment variables.
    """
    model_config = SettingsConfigDict(
        env_prefix="NOTIFY_",
        env_file=".env",
        extra="ignore",
    )
    
    slack_conn_id: str = "slack_default"
    slack_channel: str = "#data-alerts"
    teams_conn_id: str = "teams_default"
    pagerduty_conn_id: str = "pagerduty_default"
    enable_slack: bool = True
    enable_email: bool = True


# =============================================================================
# DAG Configuration Models (Pydantic BaseModel)
# =============================================================================

class DagArgs(BaseModel):
    """
    Pydantic model for Airflow default_args.
    
    Usage:
        args = DefaultArgsModel(owner="data_team", retries=3)
        dag_default_args = args.to_dict()
    """
    model_config = {"arbitrary_types_allowed": True}
    
    owner: str = "airflow"
    depends_on_past: bool = False
    email: Optional[List[str]] = None
    email_on_failure: bool = False
    email_on_retry: bool = False
    retries: int = 1
    retry_delay_minutes: int = Field(default=5, ge=1, le=60)
    start_date: datetime = Field(default_factory=lambda: datetime(2024, 1, 1))
    end_date: Optional[datetime] = None
    execution_timeout_minutes: Optional[int] = Field(default=None, ge=1)
    pool: Optional[str] = None
    queue: Optional[str] = None
    
    @field_validator("email", mode="before")
    @classmethod
    def validate_email(cls, v):
        if isinstance(v, str):
            return [e.strip() for e in v.split(",") if e.strip()]
        return v
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to Airflow-compatible default_args dictionary."""
        args = {
            "owner": self.owner,
            "depends_on_past": self.depends_on_past,
            "email_on_failure": self.email_on_failure,
            "email_on_retry": self.email_on_retry,
            "retries": self.retries,
            "retry_delay": timedelta(minutes=self.retry_delay_minutes),
            "start_date": self.start_date,
        }
        
        if self.email:
            args["email"] = self.email
        if self.end_date:
            args["end_date"] = self.end_date
        if self.execution_timeout_minutes:
            args["execution_timeout"] = timedelta(minutes=self.execution_timeout_minutes)
        if self.pool:
            args["pool"] = self.pool
        if self.queue:
            args["queue"] = self.queue
            
        return args
    
    @classmethod
    def from_settings(cls, settings: AirflowSettings, **overrides) -> "DefaultArgsModel":
        """Create DefaultArgsModel from AirflowSettings with optional overrides."""
        return cls(
            owner=overrides.get("owner", settings.default_owner),
            depends_on_past=overrides.get("depends_on_past", settings.depends_on_past),
            email=overrides.get("email", settings.alert_emails or None),
            email_on_failure=overrides.get("email_on_failure", settings.email_on_failure),
            email_on_retry=overrides.get("email_on_retry", settings.email_on_retry),
            retries=overrides.get("retries", settings.default_retries),
            retry_delay_minutes=overrides.get("retry_delay_minutes", settings.default_retry_delay_minutes),
            start_date=overrides.get("start_date", settings.default_start_date),
            pool=overrides.get("pool", settings.default_pool),
            queue=overrides.get("queue", settings.default_queue),
            **{k: v for k, v in overrides.items() if k not in [
                "owner", "depends_on_past", "email", "email_on_failure",
                "email_on_retry", "retries", "retry_delay_minutes", "start_date",
                "pool", "queue"
            ]}
        )


class DAGConfig(BaseModel):
    """
    Pydantic configuration class for DAG settings.
    
    Usage:
        config = DAGConfig(
            dag_id="my_pipeline",
            schedule="@daily",
            owner="data_team",
            tags=["production", "etl"]
        )
        
        @dag(**config.to_dag_kwargs())
        def my_pipeline():
            ...
    """
    model_config = {"arbitrary_types_allowed": True}
    
    # Required
    dag_id: str = Field(..., min_length=1, max_length=250)
    
    # Scheduling
    schedule: Optional[str] = "@daily"
    start_date: datetime = Field(default_factory=lambda: datetime(2024, 1, 1))
    end_date: Optional[datetime] = None
    catchup: bool = False
    
    # Metadata
    owner: str = "airflow"
    description: str = ""
    tags: List[str] = Field(default_factory=list)
    
    # Execution
    max_active_runs: int = Field(default=1, ge=1)
    max_active_tasks: Optional[int] = Field(default=None, ge=1)
    concurrency: Optional[int] = Field(default=None, ge=1)
    dagrun_timeout_minutes: Optional[int] = Field(default=None, ge=1)
    
    # Retry settings
    retries: int = Field(default=1, ge=0)
    retry_delay_minutes: int = Field(default=5, ge=1, le=60)
    
    # Advanced
    render_template_as_native_obj: bool = False
    is_paused_upon_creation: Optional[bool] = None
    
    @field_validator("dag_id")
    @classmethod
    def validate_dag_id(cls, v):
        """Ensure dag_id follows naming conventions."""
        if not v.replace("_", "").replace("-", "").isalnum():
            raise ValueError("dag_id must contain only alphanumeric characters, underscores, or hyphens")
        return v
    
    @field_validator("tags", mode="before")
    @classmethod
    def validate_tags(cls, v):
        """Parse comma-separated tags string."""
        if isinstance(v, str):
            return [t.strip() for t in v.split(",") if t.strip()]
        return v
    
    @model_validator(mode="after")
    def validate_dates(self):
        """Ensure end_date is after start_date."""
        if self.end_date and self.end_date <= self.start_date:
            raise ValueError("end_date must be after start_date")
        return self
    
    def get_default_args(self) -> Dict[str, Any]:
        """Generate default_args dictionary."""
        return DefaultArgsModel(
            owner=self.owner,
            retries=self.retries,
            retry_delay_minutes=self.retry_delay_minutes,
            start_date=self.start_date,
            end_date=self.end_date,
        ).to_dict()
    
    def to_dag_kwargs(self) -> Dict[str, Any]:
        """Convert config to DAG decorator kwargs."""
        kwargs = {
            "dag_id": self.dag_id,
            "schedule": self.schedule,
            "default_args": self.get_default_args(),
            "description": self.description,
            "tags": self.tags,
            "catchup": self.catchup,
            "max_active_runs": self.max_active_runs,
            "render_template_as_native_obj": self.render_template_as_native_obj,
        }
        
        if self.max_active_tasks:
            kwargs["max_active_tasks"] = self.max_active_tasks
        if self.concurrency:
            kwargs["concurrency"] = self.concurrency
        if self.dagrun_timeout_minutes:
            kwargs["dagrun_timeout"] = timedelta(minutes=self.dagrun_timeout_minutes)
        if self.is_paused_upon_creation is not None:
            kwargs["is_paused_upon_creation"] = self.is_paused_upon_creation
            
        return kwargs
    
    def with_overrides(self, **overrides) -> "DAGConfig":
        """Create a new DAGConfig with specified overrides."""
        data = self.model_dump()
        data.update(overrides)
        return DAGConfig(**data)


class TaskConfig(BaseModel):
    """
    Pydantic configuration for individual task settings.
    
    Usage:
        task_config = TaskConfig(
            task_id="extract_data",
            retries=3,
            pool="io_pool"
        )
    """
    task_id: str = Field(..., min_length=1)
    retries: Optional[int] = Field(default=None, ge=0)
    retry_delay_minutes: Optional[int] = Field(default=None, ge=1)
    execution_timeout_minutes: Optional[int] = Field(default=None, ge=1)
    pool: Optional[str] = None
    queue: Optional[str] = None
    priority_weight: int = Field(default=1, ge=1)
    trigger_rule: str = "all_success"
    
    def to_task_kwargs(self) -> Dict[str, Any]:
        """Convert to task decorator/operator kwargs."""
        kwargs = {
            "task_id": self.task_id,
            "priority_weight": self.priority_weight,
            "trigger_rule": self.trigger_rule,
        }
        
        if self.retries is not None:
            kwargs["retries"] = self.retries
        if self.retry_delay_minutes:
            kwargs["retry_delay"] = timedelta(minutes=self.retry_delay_minutes)
        if self.execution_timeout_minutes:
            kwargs["execution_timeout"] = timedelta(minutes=self.execution_timeout_minutes)
        if self.pool:
            kwargs["pool"] = self.pool
        if self.queue:
            kwargs["queue"] = self.queue
            
        return kwargs


# =============================================================================
# Pre-defined Configuration Presets
# =============================================================================

class ConfigPresets:
    """Pre-defined DAG configuration presets for common patterns."""
    
    @staticmethod
    def daily_batch(**overrides) -> DAGConfig:
        """Daily batch processing DAG preset."""
        return DAGConfig(
            dag_id=overrides.pop("dag_id", "daily_batch"),
            schedule="@daily",
            catchup=False,
            max_active_runs=1,
            **overrides
        )
    
    @staticmethod
    def hourly_streaming(**overrides) -> DAGConfig:
        """Hourly streaming/micro-batch DAG preset."""
        return DAGConfig(
            dag_id=overrides.pop("dag_id", "hourly_streaming"),
            schedule="@hourly",
            catchup=False,
            max_active_runs=3,
            **overrides
        )
    
    @staticmethod
    def manual_trigger(**overrides) -> DAGConfig:
        """Manually triggered DAG preset."""
        return DAGConfig(
            dag_id=overrides.pop("dag_id", "manual_trigger"),
            schedule=None,
            catchup=False,
            max_active_runs=1,
            is_paused_upon_creation=False,
            **overrides
        )
    
    @staticmethod
    def realtime(**overrides) -> DAGConfig:
        """Real-time processing DAG preset (every 5 minutes)."""
        return DAGConfig(
            dag_id=overrides.pop("dag_id", "realtime"),
            schedule="*/5 * * * *",
            catchup=False,
            max_active_runs=5,
            retries=0,
            **overrides
        )
    
    @staticmethod
    def weekly_report(**overrides) -> DAGConfig:
        """Weekly reporting DAG preset."""
        return DAGConfig(
            dag_id=overrides.pop("dag_id", "weekly_report"),
            schedule="0 0 * * 0",  # Sunday at midnight
            catchup=False,
            max_active_runs=1,
            dagrun_timeout_minutes=120,
            **overrides
        )


# =============================================================================
# Helper Functions
# =============================================================================

def create_dag_args(
    owner: str = "airflow",
    retries: int = 1,
    retry_delay_minutes: int = 5,
    start_date: Optional[datetime] = None,
    email: Optional[List[str]] = None,
    email_on_failure: bool = False,
    email_on_retry: bool = False,
    **extra_args
) -> Dict[str, Any]:
    """
    Create customized DAG arguments (legacy helper function).
    
    For new code, prefer using DefaultArgsModel or DAGConfig.
    """
    return DefaultArgsModel(
        owner=owner,
        retries=retries,
        retry_delay_minutes=retry_delay_minutes,
        start_date=start_date or datetime(2024, 1, 1),
        email=email,
        email_on_failure=email_on_failure,
        email_on_retry=email_on_retry,
        **extra_args
    ).to_dict()


# Legacy constants for backward compatibility
DEFAULT_DAG_ARGS = DefaultArgsModel().to_dict()

DAILY_BATCH_CONFIG = {
    "schedule": "@daily",
    "catchup": False,
    "max_active_runs": 1,
}

HOURLY_STREAMING_CONFIG = {
    "schedule": "@hourly",
    "catchup": False,
    "max_active_runs": 3,
}

MANUAL_TRIGGER_CONFIG = {
    "schedule": None,
    "catchup": False,
    "max_active_runs": 1,
}
