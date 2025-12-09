from airflow import DAG
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator

def test_hook_basic():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE();")
    result = cursor.fetchall()
    print("BASIC QUERY RESULT:", result)

def test_hook_run():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    result = hook.run("SELECT CURRENT_TIMESTAMP();")
    print("RUN() RESULT:", result)

def test_hook_run_multiple():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    sql_list = [
        "CREATE TEMP TABLE HOOK_TEST (id INT);",
        "INSERT INTO HOOK_TEST VALUES (1);",
        "SELECT COUNT(*) FROM HOOK_TEST;"
    ]
    results = hook.run(sql_list, autocommit=True)
    print("RUN MULTIPLE RESULT:", results)

def test_hook_run_parameters():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT %(value)s AS PARAM_TEST",
        {"value": 123}
    )
    result = cursor.fetchall()
    print("PARAMETERIZED QUERY RESULT:", result)
    
    cursor.close()
    conn.close()

with DAG(
    dag_id="test_snowflake_hook_full",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    basic = PythonOperator(
        task_id="test_hook_basic",
        python_callable=test_hook_basic
    )

    run_single = PythonOperator(
        task_id="test_hook_run",
        python_callable=test_hook_run
    )

    run_multi = PythonOperator(
        task_id="test_hook_run_multiple",
        python_callable=test_hook_run_multiple
    )

    run_params = PythonOperator(
        task_id="test_hook_run_parameters",
        python_callable=test_hook_run_parameters
    )

    basic >> run_single >> run_multi >> run_params
