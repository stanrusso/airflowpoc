# Configuration module with Pydantic models
from src.settings.dag_configs import (
    # Settings (BaseSettings - loaded from environment)
    AirflowSettings,
    DataSourceSettings,
    NotificationSettings,
    # Models (BaseModel)
    DefaultArgsModel,
    DAGConfig,
    TaskConfig,
    # Presets
    ConfigPresets,
    # Legacy helpers
    DEFAULT_DAG_ARGS,
    create_dag_args,
    DAILY_BATCH_CONFIG,
    HOURLY_STREAMING_CONFIG,
    MANUAL_TRIGGER_CONFIG,
)

__all__ = [
    # Settings
    "AirflowSettings",
    "DataSourceSettings",
    "NotificationSettings",
    # Models
    "DefaultArgsModel",
    "DAGConfig",
    "TaskConfig",
    # Presets
    "ConfigPresets",
    # Legacy
    "DEFAULT_DAG_ARGS",
    "create_dag_args",
    "DAILY_BATCH_CONFIG",
    "HOURLY_STREAMING_CONFIG",
    "MANUAL_TRIGGER_CONFIG",
]

