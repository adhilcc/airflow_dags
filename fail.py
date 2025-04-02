from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='failing_dag',
    start_date=datetime(2025, 4, 1),  # Set to a past date for immediate execution
    schedule_interval=None,           # No schedule, run manually
    catchup=False,                    # Avoid backfilling
    tags=['test', 'failure'],
) as dag:

    # Define a Python function that fails
    def fail_task():
        print("This task will fail intentionally.")
        raise ValueError("Simulated failure for testing alerts!")

    # Create a failing task
    failing_task = PythonOperator(
        task_id='fail_task',
        python_callable=fail_task,
    )
