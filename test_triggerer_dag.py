from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.time_sensor import TimeSensorAsync
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="test_triggerer",
    description="Test that the Triggerer works using TimeSensorAsync (deferrable)",
    schedule=None,                               # manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["test", "triggerer", "deferrable"],
    default_args={
        "owner": "airflow",
        "retries": 0,
    },
) as dag:

    start = EmptyOperator(task_id="start")

    wait_30_seconds = TimeSensorAsync(
        task_id="wait_30_seconds_deferrable",
        target_time=(datetime.utcnow() + timedelta(seconds=60)).time(),
        poke_interval=5,
        timeout=120,
        mode="reschedule",   # required for deferral
    )

    end = EmptyOperator(task_id="end")

    start >> wait_30_seconds >> end