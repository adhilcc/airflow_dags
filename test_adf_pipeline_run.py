from airflow import DAG
from datetime import datetime
from airflow.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperator,
)
import pendulum

local_tz = pendulum.timezone("UTC")

with DAG(
    dag_id="test_adf_pipeline_run",
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    schedule=None,
    catchup=False,
    tags=["test", "azure", "adf"],
) as dag:

    trigger_pipeline = AzureDataFactoryRunPipelineOperator(
        task_id="run_adf_pipeline",
        pipeline_name="AirflowTestPipeline",
        azure_data_factory_conn_id="azure_data_factory_default",
        wait_for_termination=True,     # Airflow waits for pipeline to finish
        check_interval=10,             # Poll every 10 seconds
        timeout=600,                   # Stop waiting after 10 minutes
    )
