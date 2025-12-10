from datetime import datetime
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.providers.microsoft.azure.sensors.data_factory import AzureDataFactoryPipelineRunStatusSensor
from airflow.providers.microsoft.azure.hooks.data_factory import AzureDataFactoryHook
import os


# Environment + Defaults

ENVIRONMENT = Variable.get("AIRFLOW_OPS_DEPLOY_ENV", default_var="DEFAULT")
run_on_prem_task = (ENVIRONMENT == "AKS-DEV")

local_tz = pendulum.timezone('America/Los_Angeles')

default_args = {
    'owner': 'OPS',
    'depends_on_past': False,
    'retries': 1,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Read README.md
readme_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'README.md')
with open(readme_path, 'r') as file:
    readme_content = file.read()


with DAG(
    dag_id="DailyHealthCheck_v8",
    default_args=default_args,
    start_date=datetime(2024, 2, 9, tzinfo=local_tz),
    schedule_interval="0 */1 * * *",
    catchup=False,
    description="Health Check DAG including Worker, Snowflake & ADF Tests",
    tags=["OPS", "HEALTHCHECK"],
    doc_md=readme_content
) as dag:

    # WORKER HEALTH CHECK GROUP

    with TaskGroup("WORKER") as worker_tasks:

        for i in range(1, 7):
            BashOperator(
                task_id=f"default_worker_check_{i}",
                bash_command="echo Default Worker Check; date",
                queue="default",
            )

        if run_on_prem_task:
            BashOperator(
                task_id="onprem_worker_check",
                bash_command="echo On-Prem Worker Check; date",
                queue="default",
            )

    # SNOWFLAKE SQL OPERATOR TESTS

    with TaskGroup("SNOWFLAKE_SQL_OPERATOR_TESTS") as sql_operator_tests:

        test_auth = SQLExecuteQueryOperator(
            task_id="test_authentication",
            conn_id="snowflake_default",
            sql="SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE();",
        )

        test_select = SQLExecuteQueryOperator(
            task_id="test_select_query",
            conn_id="snowflake_default",
            sql="SELECT CURRENT_TIMESTAMP();",
        )

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

        test_stored_proc = SQLExecuteQueryOperator(
            task_id="test_stored_procedure",
            conn_id="snowflake_default",
            sql="CALL SYSTEM$WAIT(1);",
        )

        test_templated_sql = SQLExecuteQueryOperator(
            task_id="test_templated_sql",
            conn_id="snowflake_default",
            sql="SELECT '{{ ds }}' AS execution_date;",
        )

        test_auth >> test_select >> test_dml >> test_stored_proc >> test_templated_sql

    # SNOWFLAKE HOOK TESTS

    def test_private_key_auth():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        conn = hook.get_conn()
        print("Private key authentication successful!")
        conn.close()

    def test_hook_basic():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE();")
        print("BASIC QUERY RESULT:", cursor.fetchall())
        cursor.close()
        conn.close()

    def test_hook_run():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        print("RUN() RESULT:", hook.run("SELECT CURRENT_TIMESTAMP();"))

    def test_hook_run_multiple():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        sql_list = [
            "CREATE TEMP TABLE HOOK_TEST (id INT);",
            "INSERT INTO HOOK_TEST VALUES (1);",
            "SELECT COUNT(*) FROM HOOK_TEST;"
        ]
        print("MULTIPLE RESULT:", hook.run(sql_list, autocommit=True))

    def test_hook_run_parameters():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT %(value)s AS PARAM_TEST", {"value": 123})
        print("PARAM RESULT:", cursor.fetchall())
        cursor.close()
        conn.close()

    with TaskGroup("SNOWFLAKE_HOOK_TESTS") as hook_tests:

        hook_pk = PythonOperator(task_id="test_private_key_auth", python_callable=test_private_key_auth)
        hook_basic = PythonOperator(task_id="test_hook_basic", python_callable=test_hook_basic)
        hook_run = PythonOperator(task_id="test_hook_run", python_callable=test_hook_run)
        hook_multi = PythonOperator(task_id="test_hook_run_multiple", python_callable=test_hook_run_multiple)
        hook_params = PythonOperator(task_id="test_hook_run_parameters", python_callable=test_hook_run_parameters)

        hook_pk >> hook_basic >> hook_run >> hook_multi >> hook_params

    # ADF TESTS

    def adf_hook_test(**kwargs):
        hook = AzureDataFactoryHook(azure_data_factory_conn_id="azure_data_factory_default")
        pipeline = hook.get_pipeline(
            pipeline_name="AirflowTestPipeline",
            resource_group_name="rg-airflow-adf-test",
            factory_name="ad-airflow-test"
        )
        print("PIPELINE:", pipeline)

        run_id = kwargs["ti"].xcom_pull(task_ids="ADF_TESTS.run_adf_pipeline", key="run_id")

        pipeline_run = hook.get_pipeline_run(
            run_id=run_id,
            resource_group_name="rg-airflow-adf-test",
            factory_name="ad-airflow-test",
        )
        print("RUN STATUS:", pipeline_run.status)

    with TaskGroup("ADF_TESTS") as adf_tests:

        run_pipeline = AzureDataFactoryRunPipelineOperator(
            task_id="run_adf_pipeline",
            pipeline_name="AirflowTestPipeline",
            azure_data_factory_conn_id="azure_data_factory_default",
            factory_name="ad-airflow-test",
            resource_group_name="rg-airflow-adf-test",
            wait_for_termination=False,
        )

        wait_pipeline = AzureDataFactoryPipelineRunStatusSensor(
            task_id="wait_for_pipeline",
            azure_data_factory_conn_id="azure_data_factory_default",
            factory_name="ad-airflow-test",
            resource_group_name="rg-airflow-adf-test",
            run_id="{{ ti.xcom_pull(task_ids='ADF_TESTS.run_adf_pipeline', key='run_id') }}",
            poke_interval=10,
            timeout=600,
        )

        test_adf_hook = PythonOperator(
            task_id="test_adf_hook",
            python_callable=adf_hook_test,
        )

        run_pipeline >> wait_pipeline >> test_adf_hook


    worker_tasks >> sql_operator_tests >> hook_tests >> adf_tests
