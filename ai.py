from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

DBT_PROJECT_PATH = "/appz/home/airflow/dags/dbt/ai_lab"
DBT_EXECUTABLE = "/dbt_venv/bin/dbt"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 2, 24),
    "retries": 1,
}

with DAG(
    dag_id="dbt_order_update",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_PATH} && {DBT_EXECUTABLE} run --select transformations.order_update",
    )

    dbt_run

#testing_through_git_ui
