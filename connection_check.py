from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    'dbt_debug_dag',
    default_args=default_args,
    description='A DAG to run dbt debug command',
    schedule_interval=None,  # Run manually
    start_date=datetime(2025, 6, 11),
    catchup=False,
) as dag:

    # Task to run dbt debug
    run_dbt_debug = BashOperator(
        task_id='run_dbt_debug',
        bash_command='source /dbt_venv/bin/activate && cd /appz/home/airflow/dags/data-engineering && dbt debug',
    )

    # Define task dependencies (single task in this case)
    run_dbt_debug
