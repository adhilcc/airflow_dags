from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

def test_key(**_):
    hook = SnowflakeHook(snowflake_conn_id="TEST_SF")
    conn_params = hook._get_conn_params   # <-- critical
    print("Connection params successfully loaded:")
    print(conn_params)

with DAG(
    dag_id="test_snowflake_key_only",
    start_date=datetime(2023,1,1),
    schedule=None,
    catchup=False,
) as dag:

    PythonOperator(
        task_id="run_test",
        python_callable=test_key
    )
