from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# Default settings for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

# Define the DAG
with DAG(
    dag_id="hello_world_dag",
    default_args=default_args,
    schedule="@once",  # runs once when triggered
    catchup=False,
    description="A simple Hello World DAG",
    tags=["example"],
) as dag:

    start = EmptyOperator(task_id="start")

    hello = BashOperator(
        task_id="say_hello",
        bash_command='echo "Hello, Airflow! The DAG is running successfully."'
    )

    end = EmptyOperator(task_id="end")

    # Task flow
    start >> hello >> end
