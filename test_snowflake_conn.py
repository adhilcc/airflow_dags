from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_sf(**kwargs):
    hook = SnowflakeHook(snowflake_conn_id="TEST_SF")
    params = hook._get_conn_params() 
    print(params)

with DAG(
    "test_snowflake_conn",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    PythonOperator(
        task_id="run_test",
        python_callable=test_sf
    )
