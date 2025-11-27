from datetime import datetime
from airflow.decorators import task, dag
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# Default settings for the DAG


default_args = {
    'owner': 'Stan',
    'start_date': datetime(2024, 2, 12),
    'retries': 1,
    'random_thing': '7'
}
@dag(
        default_args=default_args, 
        schedule="@once", 
        description="Simple Pipeline with Titanic", 
        catchup=False, 
        tags=['Titanic']
)
def main():

    start = EmptyOperator(task_id="start")

    hello = BashOperator(
        task_id="say_hello",
        bash_command='echo "Hello, Airflow! The DAG is running successfully."'
    )

    end = EmptyOperator(task_id="end")

    # Task flow
    start >> hello >> end


execution = main()