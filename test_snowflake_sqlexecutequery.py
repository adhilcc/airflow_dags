from airflow import DAG
from datetime import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="test_snowflake_sqlexecutequery_full",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    # Authentication Test
    test_auth = SQLExecuteQueryOperator(
        task_id="test_authentication",
        conn_id="snowflake_default",
        sql="SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE();",
    )

    # SELECT Query Test
    test_select = SQLExecuteQueryOperator(
        task_id="test_select_query",
        conn_id="snowflake_default",
        sql="SELECT CURRENT_TIMESTAMP();",
    )

    # DDL + DML Test
    test_dml = SQLExecuteQueryOperator(
        task_id="test_dml_query",
        conn_id="snowflake_default",
        sql="""
            CREATE TEMP TABLE IF NOT EXISTS SF_TEST_TABLE (id INT, name STRING);
            INSERT INTO SF_TEST_TABLE VALUES (1, 'AIRFLOW'), (2, 'TEST');
            SELECT COUNT(*) FROM SF_TEST_TABLE;
        """,
        split_statements=True,
    )

    # Stored Procedure Test
    test_stored_proc = SQLExecuteQueryOperator(
        task_id="test_stored_procedure",
        conn_id="snowflake_default",
        sql="CALL SYSTEM$WAIT(1);"
    )

    # Jinja Template Test
    test_templated_sql = SQLExecuteQueryOperator(
        task_id="test_templated_sql",
        conn_id="snowflake_default",
        sql="SELECT '{{ ds }}' AS execution_date;",
    )

    test_auth >> test_select >> test_dml >> test_stored_proc >> test_templated_sql
