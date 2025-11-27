from airflow.decorators import task


@task
def sample_task():
    print("This is a sample task.")