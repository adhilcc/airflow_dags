from __future__ import annotations

import logging

import pendulum

from airflow.sdk import DAG
from airflow.configuration import conf
from airflow.providers.standard.operators.python import PythonOperator

log = logging.getLogger(__name__)

# Diagnostic DAG to reproduce/verify the email-notification URL issue.
# The link embedded in Airflow's task emails comes from TaskInstance.log_url,
# which in Airflow 3 is built from conf.get("api", "base_url").
# This DAG runs a task on the CELERY WORKER and prints exactly what that
# base_url + reconstructed log_url resolve to on the worker.


def show_base_url(**context):
    ti = context["ti"]
    base_url = conf.get("api", "base_url", fallback="http://localhost:8080/")
    exec_url = conf.get("core", "execution_api_server_url", fallback="<unset>")

    log.info("=================== EMAIL-URL REPRO ===================")
    log.info("RESOLVED [api] base_url                = %r", base_url)
    log.info("RESOLVED [core] execution_api_server_url = %r", exec_url)

    # Reconstruct the link the same way TaskInstance.log_url does.
    reconstructed = (
        f"{base_url.rstrip('/')}/dags/{ti.dag_id}/runs/{ti.run_id}/tasks/{ti.task_id}"
    )
    log.info("RECONSTRUCTED log_url (as email would show) = %s", reconstructed)

    # Also try the real property if the runtime TI exposes it.
    try:
        log.info("ti.log_url = %s", ti.log_url)
    except Exception as e:  # noqa: BLE001
        log.info("ti.log_url not available on runtime TI: %s", e)
    log.info("=======================================================")


def always_fails(**context):
    raise RuntimeError("Intentional failure to exercise the email_on_failure path.")


with DAG(
    dag_id="zz_email_url_repro",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["diagnostic", "email-url"],
    default_args={
        "email": ["repro-test@example.com"],
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 0,
    },
) as dag:
    show = PythonOperator(
        task_id="show_base_url",
        python_callable=show_base_url,
    )

    fail = PythonOperator(
        task_id="always_fails",
        python_callable=always_fails,
    )

    show >> fail
