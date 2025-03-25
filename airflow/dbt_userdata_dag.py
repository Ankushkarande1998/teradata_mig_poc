from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define S3 bucket and file details
S3_BUCKET = "test-kr9948"
S3_KEY = "airflow_dags/dbt_project/sample-dbt-poc_new.zip"  # Your ZIP file path
LOCAL_ZIP_PATH = "/tmp/sample-dbt-poc_new.zip"  # Temporary zip storage
DBT_PROJECT_DIR = "~/sample-dbt-poc_new"  # Extracted folder in home directory
DBT_VENV_PATH = "${AIRFLOW_HOME}/dbt_venv"  # Path to dbt virtual environment

# Define default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 3, 1, tz="UTC"),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id="databricks_dbt",
    default_args=default_args,
    description="DAG to pull dbt project from S3, unzip, and run dbt transformations",
    schedule_interval=None,
    catchup=False,
    tags=["dbt", "databricks", "postgres"],
) as dag:

    # Task 1: Download dbt project ZIP from S3
    download_dbt_project = BashOperator(
        task_id="download_dbt_project",
        bash_command=f"aws s3 cp s3://{S3_BUCKET}/{S3_KEY} {LOCAL_ZIP_PATH}"
    )

    # Task 2: Extract dbt project ZIP into home directory
    extract_dbt_project = BashOperator(
        task_id="extract_dbt_project",
        bash_command=f"mkdir -p ~/ && unzip -o {LOCAL_ZIP_PATH} -d ~/"
    )

    # Task 3: Run dbt transformations for Databricks
    dbt_run_databrick = BashOperator(
        task_id="dbt_run_databrick",
        bash_command=(
            f"source {DBT_VENV_PATH}/bin/activate && "
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir . --profile databricks_transformation --target dev"
        ),
    )

    # Define task dependencies
    download_dbt_project >> extract_dbt_project >> dbt_run_databrick
