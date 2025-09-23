from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

def my_python_task():
    print("Hello from Python")

with DAG(
    dag_id='sample_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['example'],
) as dag:

    python_task = PythonOperator(
        task_id='python_task',
        python_callable=my_python_task
    )

    failing_bash_task = BashOperator(
        task_id='failing_bash_task',
        bash_command='exit 1'
    )

    python_task >> failing_bash_task

#testing
