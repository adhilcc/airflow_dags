from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

def test_sf(**kwargs):
    hook = SnowflakeHook(snowflake_conn_id="TEST_SF")
    conn = hook.get_conn()    # <-- ERROR happens here in 2.11
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    print(cursor.fetchall())

with DAG(
    dag_id="test_snowflake_conn",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    PythonOperator(
        task_id="run_test",
        python_callable=test_sf
    )
