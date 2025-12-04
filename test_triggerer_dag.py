from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.time_sensor import TimeSensorAsync  # deferrable sensor
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='test_triggerer',
    default_args=default_args,
    description='Simple DAG to test Airflow Triggerer with deferrable sensor',
    schedule_interval=None,  # manual trigger only
    start_date=datetime(2025, 12, 4),
    catchup=False,
    tags=['test', 'triggerer'],
) as dag:

    start = EmptyOperator(task_id='start')

    # This sensor waits 50 seconds asynchronously → should defer to Triggerer
    wait_async = TimeSensorAsync(
        task_id='wait_30_seconds_async',
        target_time="{{ execution_date.add(seconds=50).time() }}",  # wait 30s from start
        poke_interval=5,  # check every 5s
        timeout=60,       # max wait 60s
        mode='reschedule',  # required for deferral
    )

    end = EmptyOperator(task_id='end')

    start >> wait_async >> end