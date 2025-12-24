from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


with DAG(
    dag_id="snowflake_operator_test",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    run_snowflake = SQLExecuteQueryOperator(
        task_id="run_snowflake_op",
        conn_id="snowflake_default",
        sql="SELECT CURRENT_TIMESTAMP();"
    )
