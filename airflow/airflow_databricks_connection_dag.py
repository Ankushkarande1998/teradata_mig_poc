from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 21),
}

with DAG(
    'databricks_airflow_connection_new',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=['dbt', 'databricks']
) as dag:

    run_databricks_job = DatabricksRunNowOperator(
        task_id="run_databricks_job",
        databricks_conn_id="databricks_default",
        job_id=695574829024822  # Your Databricks job ID
    )

    run_databricks_job