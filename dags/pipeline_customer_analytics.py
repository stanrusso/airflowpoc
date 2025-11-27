"""
Example Pipeline 2: Customer Analytics Pipeline
Demonstrates using reusable tasks with different configurations.
"""
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator

# Import reusable components
from src.tasks.data_tasks import read_csv_data, validate_data, transform_data, aggregate_results
from src.tasks.notification_tasks import send_success_notification
from src.settings.dag_configs import create_dag_args


# Custom DAG arguments
dag_args = create_dag_args(
    owner="analytics_team",
    retries=2,
    retry_delay_minutes=10,
    start_date=datetime(2024, 1, 1),
)


@dag(
    dag_id="pipeline_customer_analytics",
    default_args=dag_args,
    schedule="@weekly",
    description="Customer analytics pipeline with reusable components",
    catchup=False,
    tags=["analytics", "customers", "reusable"],
)
def pipeline_customer_analytics():
    """
    Customer analytics pipeline demonstrating parallel task execution.
    """
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    # Read customer data (reusing the generic read task)
    customer_data = read_csv_data(
        source="https://raw.githubusercontent.com/stanrusso/airflowpoc/main/dag_data/data.csv",
        separator=";"
    )
    
    # Validate the data
    validated = validate_data(
        data_dict=customer_data,
        required_columns=["Survived", "Sex", "Age", "Pclass"]
    )
    
    # Multiple parallel transformations using the same reusable task
    # Analysis 1: Age demographics
    age_analysis = transform_data(
        data_dict=validated,
        transformations={
            "select": ["Age", "Survived"],
            "filter": {"column": "Age", "value": 0, "operator": "gt"}
        }
    )
    
    # Analysis 2: Class distribution
    class_analysis = transform_data(
        data_dict=validated,
        transformations={
            "groupby": {"columns": ["Pclass"], "agg": "count"}
        }
    )
    
    # Analysis 3: Gender survival rate
    gender_analysis = transform_data(
        data_dict=validated,
        transformations={
            "filter": {"column": "Survived", "value": 1, "operator": "eq"},
            "groupby": {"columns": ["Sex"], "agg": "count"}
        }
    )
    
    # Custom aggregation task
    @task
    def create_report(age_data, class_data, gender_data):
        """Create a comprehensive report from all analyses."""
        report = {
            "age_analysis_rows": age_data["row_count"],
            "class_distribution": class_data["data"],
            "gender_survival": gender_data["data"],
            "report_generated": datetime.now().isoformat(),
        }
        print(f"Report generated with {len(report)} sections")
        return report
    
    report = create_report(age_analysis, class_analysis, gender_analysis)
    
    # Send notification using reusable task
    notify = send_success_notification(
        dag_id="pipeline_customer_analytics",
        message="Customer analytics report generated successfully!"
    )
    
    # Task flow
    start >> customer_data >> validated
    validated >> [age_analysis, class_analysis, gender_analysis]
    [age_analysis, class_analysis, gender_analysis] >> report >> notify >> end


# Instantiate the DAG
pipeline_customer_analytics()
