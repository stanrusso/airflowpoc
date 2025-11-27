"""
Example Pipeline 1: Data Processing Pipeline
Demonstrates using reusable tasks from the common module.
"""
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator

# Import reusable components
from src.tasks.data_tasks import read_csv_data, validate_data, transform_data
from src.tasks.utility_tasks import log_start, log_end
from src.settings.dag_configs import DAGConfig


# Create configuration using DAGConfig class
config = DAGConfig(
    dag_id="pipeline_data_processing",
    schedule="@daily",
    owner="data_team",
    description="Example data processing pipeline using reusable tasks",
    tags=["example", "data-processing", "reusable"],
)


@dag(**config.to_dag_kwargs())
def pipeline_data_processing():
    """
    Data processing pipeline that demonstrates reusable task patterns.
    """
    # Start/End markers
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    # Use reusable logging tasks
    start_log = log_start(dag_id=config.dag_id)
    
    # Use reusable data reading task
    raw_data = read_csv_data(
        source="https://raw.githubusercontent.com/stanrusso/airflowpoc/main/dag_data/data.csv",
        separator=";"
    )
    
    # Use reusable validation task
    validated_data = validate_data(
        data_dict=raw_data,
        required_columns=["Survived", "Sex", "Age"]
    )
    
    # Use reusable transformation task - get survivors
    survivors = transform_data(
        data_dict=validated_data,
        transformations={
            "filter": {"column": "Survived", "value": 1, "operator": "eq"},
            "select": ["Survived", "Sex", "Age"]
        }
    )
    
    # Use reusable transformation task - group by sex
    survivors_by_sex = transform_data(
        data_dict=survivors,
        transformations={
            "groupby": {"columns": ["Sex"], "agg": "count"}
        }
    )
    
    # Custom task specific to this DAG
    @task
    def summarize_results(survivors_data, grouped_data):
        print(f"Total survivors: {survivors_data['row_count']}")
        print(f"Grouped data columns: {grouped_data['columns']}")
        return {
            "total_survivors": survivors_data['row_count'],
            "summary": "Pipeline completed successfully"
        }
    
    summary = summarize_results(survivors, survivors_by_sex)
    
    # Use reusable end logging
    end_log = log_end(dag_id=config.dag_id, start_info=start_log)
    
    # Define task dependencies
    start >> start_log >> raw_data >> validated_data >> survivors
    survivors >> [survivors_by_sex, summary] >> end_log >> end


# Instantiate the DAG
pipeline_data_processing()
