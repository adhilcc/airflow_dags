from airflow import DAG
from datetime import datetime
import pendulum

from airflow.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperator,
)
from airflow.providers.microsoft.azure.sensors.data_factory import (
    AzureDataFactoryPipelineRunStatusSensor,
)
from airflow.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryHook,
)
from airflow.operators.python import PythonOperator

local_tz = pendulum.timezone("UTC")

# -------------------------------
# Test 3: Azure Data Factory Hook
# -------------------------------
def test_adf_hook():
    hook = AzureDataFactoryHook(azure_data_factory_conn_id="azure_data_factory_default")

    # Get pipeline details
    pipeline = hook.get_pipeline(
        pipeline_name="AirflowTestPipeline",
        resource_group_name="rg-airflow-adf-test",
        factory_name="ad-airflow-test"
    )
    print("PIPELINE DEFINITION:", pipeline)

    # Get pipeline run result
    run_id = kwargs['ti'].xcom_pull(task_ids='run_adf_pipeline', key='run_id')
    pipeline_run = hook.get_pipeline_run(
        run_id=run_id,
        resource_group_name="rg-airflow-adf-test",
        factory_name="ad-airflow-test",
    )

    print("PIPELINE RUN STATUS:", pipeline_run.status)


with DAG(
    dag_id="test_adf_full",
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    schedule=None,
    catchup=False,
    tags=["test", "azure", "adf"],
) as dag:

    # --------------------------------------------------------
    # TEST OPERATOR: Run ADF Pipeline
    # --------------------------------------------------------
    trigger_pipeline = AzureDataFactoryRunPipelineOperator(
        task_id="run_adf_pipeline",
        pipeline_name="AirflowTestPipeline",
        azure_data_factory_conn_id="azure_data_factory_default",
        factory_name="ad-airflow-test",
        resource_group_name="rg-airflow-adf-test",
        wait_for_termination=False,  # we will test sensor separately
    )

    # --------------------------------------------------------
    # TEST SENSOR: Wait for pipeline completion
    # --------------------------------------------------------
    wait_for_pipeline = AzureDataFactoryPipelineRunStatusSensor(
        task_id="wait_for_pipeline",
        azure_data_factory_conn_id="azure_data_factory_default",
        factory_name="ad-airflow-test",
        resource_group_name="rg-airflow-adf-test",
        run_id="{{ ti.xcom_pull(task_ids='run_adf_pipeline', key='run_id') }}",
        poke_interval=10,
        timeout=600,
    )

    # --------------------------------------------------------
    # TEST HOOK: list pipelines + trigger pipeline via hook
    # --------------------------------------------------------
    hook_test_task = PythonOperator(
        task_id="test_adf_hook",
        python_callable=test_adf_hook,
    )

    # Task dependencies
    trigger_pipeline >> wait_for_pipeline >> hook_test_task
