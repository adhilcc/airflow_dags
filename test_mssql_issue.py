from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_mssql():
    hook = MsSqlHook(mssql_conn_id="my_mssql_conn")
    hook.get_pandas_df("SELECT 1")

with DAG(
    dag_id="test_mssql_issue",
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False
):
    PythonOperator(task_id="run", python_callable=test_mssql)
