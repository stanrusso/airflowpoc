"""
Example Pipeline 3: ETL Pipeline using TaskGroups
Demonstrates using reusable TaskGroups for modular ETL workflows.
"""
from datetime import datetime
from airflow.decorators import dag, task, task_group
from airflow.providers.standard.operators.empty import EmptyOperator

from src.settings.dag_configs import DAGConfig, DAILY_BATCH_CONFIG


config = DAGConfig(
    dag_id="pipeline_etl_modular",
    owner="data_engineering",
    description="Modular ETL pipeline using TaskGroups",
    tags=["etl", "modular", "taskgroups"],
    **DAILY_BATCH_CONFIG
)


@dag(**config.to_dag_kwargs())
def pipeline_etl_modular():
    """
    Modular ETL pipeline demonstrating TaskGroup patterns.
    """
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    # Define a reusable extract TaskGroup
    @task_group(group_id="extract_phase")
    def extract_phase():
        @task
        def extract_source_a():
            """Extract data from source A."""
            return {"source": "A", "records": 100}
        
        @task
        def extract_source_b():
            """Extract data from source B."""
            return {"source": "B", "records": 200}
        
        @task
        def combine_extracts(source_a, source_b):
            """Combine extracted data from multiple sources."""
            return {
                "combined_records": source_a["records"] + source_b["records"],
                "sources": [source_a["source"], source_b["source"]]
            }
        
        a = extract_source_a()
        b = extract_source_b()
        combined = combine_extracts(a, b)
        return combined
    
    # Define a reusable transform TaskGroup
    @task_group(group_id="transform_phase")
    def transform_phase(raw_data):
        @task
        def clean_data(data):
            """Clean and normalize the data."""
            return {
                **data,
                "cleaned": True,
                "null_records_removed": 5
            }
        
        @task
        def enrich_data(data):
            """Enrich data with additional attributes."""
            return {
                **data,
                "enriched": True,
                "new_columns_added": ["timestamp", "source_id"]
            }
        
        @task
        def validate_transform(data):
            """Validate transformed data."""
            return {
                **data,
                "validated": True,
                "validation_passed": True
            }
        
        cleaned = clean_data(raw_data)
        enriched = enrich_data(cleaned)
        validated = validate_transform(enriched)
        return validated
    
    # Define a reusable load TaskGroup
    @task_group(group_id="load_phase")
    def load_phase(transformed_data):
        @task
        def load_to_warehouse(data):
            """Load data to data warehouse."""
            print(f"Loading {data['combined_records']} records to warehouse")
            return {"destination": "warehouse", "status": "success"}
        
        @task
        def load_to_cache(data):
            """Load summary to cache for quick access."""
            print(f"Caching summary data")
            return {"destination": "cache", "status": "success"}
        
        @task
        def generate_load_report(warehouse_result, cache_result):
            """Generate final load report."""
            return {
                "warehouse": warehouse_result,
                "cache": cache_result,
                "completed_at": datetime.now().isoformat()
            }
        
        warehouse = load_to_warehouse(transformed_data)
        cache = load_to_cache(transformed_data)
        report = generate_load_report(warehouse, cache)
        return report
    
    # Orchestrate the pipeline
    extracted = extract_phase()
    transformed = transform_phase(extracted)
    loaded = load_phase(transformed)
    
    # Define flow
    start >> extracted >> transformed >> loaded >> end


# Instantiate the DAG
pipeline_etl_modular()
