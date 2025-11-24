from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum

local_tz = pendulum.timezone("UTC")

def test_connection():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_test_conn")
    conn = hook.get_conn()  # This line will fail in 2.11 if key is malformed
    print("Successfully connected to Snowflake!")
    conn.close()

with DAG(
    dag_id="test_snowflake_private_key_padding",
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule=None,
    catchup=False,
    tags=["test", "snowflake"],
) as dag:
    test_task = PythonOperator(
        task_id="test_snowflake_connection",
        python_callable=test_connection,
    )