from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

def print_password():
    password = Variable.get("password", default_var="NOT SET")
    print(f"Password: {password}")

with DAG(
    "echo_airflow_password",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    echo_password = PythonOperator(
        task_id="echo_password",
        python_callable=print_password,
    )
