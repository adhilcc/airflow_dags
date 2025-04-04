from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime

# Retrieve password variable from Airflow
password = Variable.get("command", default_var="No password set")

# Define DAG
with DAG(
    dag_id="echo_airflow_variable_password",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Run manually
    catchup=False,
) as dag:

    echo_password = BashOperator(
        task_id="echo_password",
        bash_command=f'echo "Password: {password}"',
    )

    echo_password
