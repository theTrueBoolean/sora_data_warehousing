import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)
sys.path.append(os.path.join(parent_dir, "scripts"))
sys.path.append(os.path.join(parent_dir, "utils"))


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from scripts.extract import extract_and_validate
from scripts.transform import clean_data, star_schema_transformation
from scripts.load_raw import create_raw_schema, load_to_raw
from scripts.load_staging import create_and_refresh_materialized_views
from scripts.star_schema import create_staging_star_schema, populate_dimensions, populate_fact_table, load_prod_schema
from scripts.data_quality import data_quality_checks_raw, data_quality_checks_staging, run_prod_validation
from utils.logger import logger
from models.data_models import FloatDataModel, ClickUpDataModel
import pandas as pd

# File paths for raw data
#TODO: Move these to config
float_file_path = "data/raw/Float - allocations.csv"
clickup_file_path = "data/raw/ClickUp - clickup.csv"

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
    "execution_timeout": timedelta(
        seconds=120
    ),
    "start_date": days_ago(1),
    "catchup": False,
}

# Initialize the DAG
with DAG(
    "etl_time_tracking_pipeline",
    default_args=default_args,
    description="End-to-end ETL pipeline for Float and ClickUp datasets",
    schedule_interval="@daily",
) as dag:

    # Extract, validate and clean Float data
    def extract_float_data():
        float_data = extract_and_validate(float_file_path, FloatDataModel)
        float_data = clean_data(float_data, "Float Data")
        pd.DataFrame(float_data).to_csv("data/processed/float_cleaned.csv", index=False)

    extract_float_data_task = PythonOperator(
        task_id="extract_float_data",
        python_callable=extract_float_data,
        on_failure_callback=lambda context: logger.error(f"Task failed: {context['task_instance'].task_id}"),
    )

    # extract validate and clean ClickUp data
    def extract_clickup_data():
        clickup_data = extract_and_validate(clickup_file_path, ClickUpDataModel)
        clickup_data = clean_data(clickup_data, "ClickUp Data")
        pd.DataFrame(clickup_data).to_csv("data/processed/clickup_cleaned.csv", index=False)

    extract_clickup_data_task = PythonOperator(
        task_id="extract_clickup_data",
        python_callable=extract_clickup_data,
        on_failure_callback=lambda context: logger.error(f"Task failed: {context['task_instance'].task_id}"),
    )

    # create raw schema tables
    create_raw_schema_task = PythonOperator(
        task_id="create_raw_schema",
        python_callable=create_raw_schema,
    )

    # Load Float data into raw schema
    load_float_data_task = PythonOperator(
        task_id="load_float_data",
        python_callable=lambda: load_to_raw(pd.read_csv("data/processed/float_cleaned.csv"), "float_allocations"),
    )

    # Load ClickUp data into raw schema
    load_clickup_data_task = PythonOperator(
        task_id="load_clickup_data",
        python_callable=lambda: load_to_raw(pd.read_csv("data/processed/clickup_cleaned.csv"), "clickup_timesheets"),
    )

    # Run data quality checks
    raw_data_quality_checks_task = PythonOperator(
        task_id="run_raw_data_quality_checks",
        python_callable=data_quality_checks_raw,
    )

    # create or refresh staging materialized views
    run_materialized_views_task = PythonOperator(
        task_id="create_staging_mv",
        python_callable=create_and_refresh_materialized_views,
    )

    # create star schema in staging
    create_star_schema_task = PythonOperator(
        task_id="create_star_schema",
        python_callable=create_staging_star_schema,
    )

    # Populate dimension tables
    populate_dimensions_task = PythonOperator(
        task_id="populate_dimensions",
        python_callable=populate_dimensions,
    )

    # Populate fact table
    populate_fact_table_task = PythonOperator(
        task_id="populate_fact_table",
        python_callable=populate_fact_table,
    )

    # Run data quality checks on staging data
    staging_data_quality_checks_task = PythonOperator(
        task_id="run_staging_data_quality_checks",
        python_callable=data_quality_checks_staging,
    )

    # Load star schema to prod
    load_prod_schema_task = PythonOperator(
        task_id="load_prod_schema",
        python_callable=load_prod_schema,
    )

    # Final validation check
    prod_validation_task = PythonOperator(
        task_id="run_prod_validation",
        python_callable=run_prod_validation,
    )

    # Send success notification (placeholder)
    def send_success_notification():
        logger.info("ETL pipeline completed successfully!")

    send_success_notification_task = PythonOperator(
        task_id="send_success_notification",
        python_callable=send_success_notification,
    )

    # Define task dependencies
    (
        [extract_float_data_task, extract_clickup_data_task]
        >> create_raw_schema_task
        >> [load_float_data_task, load_clickup_data_task]
        >> raw_data_quality_checks_task
        >> run_materialized_views_task
        >> create_star_schema_task
        >> populate_dimensions_task
        >> populate_fact_table_task
        >> staging_data_quality_checks_task
        >> load_prod_schema_task
        >> prod_validation_task
        >> send_success_notification_task
    )
