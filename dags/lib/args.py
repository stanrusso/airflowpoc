from pydantic import BaseModel

class DagArgs(BaseModel):
    owner: str = "airflow"
    retries: int = 1
    retry_delay_minutes: int = 5