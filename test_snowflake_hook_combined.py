from airflow import DAG
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
import pendulum

# Use UTC or your preferred timezone
local_tz = pendulum.timezone("UTC")

# -------------------------------------------------------------------
# 1️⃣ PRIVATE KEY / CONNECTION VALIDATION TEST
# -------------------------------------------------------------------
def test_private_key_auth():
    """
    Validates Snowflake Key-Pair Authentication after Airflow 2.11 upgrade.
    Ensures private_key_content (base64) is correctly decoded.
    """
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    print("Private key authentication successful!")
    conn.close()


# -------------------------------------------------------------------
# 2️⃣ BASIC QUERY TEST
# -------------------------------------------------------------------
def test_hook_basic():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE();")
    result = cursor.fetchall()
    print("BASIC QUERY RESULT:", result)

    cursor.close()
    conn.close()


# -------------------------------------------------------------------
# 3️⃣ hook.run() COMPATIBILITY TEST
# -------------------------------------------------------------------
def test_hook_run():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    result = hook.run("SELECT CURRENT_TIMESTAMP();")
    print("RUN() RESULT:", result)


# -------------------------------------------------------------------
# 4️⃣ MULTIPLE STATEMENT EXECUTION TEST
# -------------------------------------------------------------------
def test_hook_run_multiple():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    sql_list = [
        "CREATE TEMP TABLE HOOK_TEST (id INT);",
        "INSERT INTO HOOK_TEST VALUES (1);",
        "SELECT COUNT(*) FROM HOOK_TEST;"
    ]
    results = hook.run(sql_list, autocommit=True)
    print("RUN MULTIPLE RESULT:", results)


# -------------------------------------------------------------------
# 5️⃣ PARAMETERIZED QUERY TEST (Pyformat syntax)
# -------------------------------------------------------------------
def test_hook_run_parameters():
    """
    Validates parameter binding using pyformat syntax:
    SELECT %(value)s → {"value": 123}

    This is the ONLY parameter syntax supported by SnowflakeHook
    after Airflow 2.11 + Provider 6.x + Connector v3.x
    """
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


# -------------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------------
with DAG(
    dag_id="test_snowflake_hook_combined",
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    schedule=None,
    catchup=False,
    tags=["snowflake", "test", "hook"],
) as dag:

    test_auth = PythonOperator(
        task_id="test_private_key_auth",
        python_callable=test_private_key_auth,
    )

    basic = PythonOperator(
        task_id="test_hook_basic",
        python_callable=test_hook_basic,
    )

    run_single = PythonOperator(
        task_id="test_hook_run",
        python_callable=test_hook_run,
    )

    run_multi = PythonOperator(
        task_id="test_hook_run_multiple",
        python_callable=test_hook_run_multiple,
    )

    run_params = PythonOperator(
        task_id="test_hook_run_parameters",
        python_callable=test_hook_run_parameters,
    )

    # Order of execution
    test_auth >> basic >> run_single >> run_multi >> run_params
