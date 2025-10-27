"""
test_fernet_secret_dag.py

Purpose:
- Demonstrate Fernet key encryption in Airflow
- Read a secret from Airflow Variable
- Print it in logs
- Compare DB state before/after Fernet key
"""

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_secret():
    secret = Variable.get("test_fernet_secret", default_var="NOT_FOUND")
    print(f"SECRET FROM VARIABLE: {secret}")
    # Also return for XCom (optional)
    return secret

with DAG(
    dag_id="test_fernet_secret",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Manual trigger
    catchup=False,
    tags=["security", "fernet", "test"],
    doc_md="""
    # Fernet Key Test DAG

    This DAG reads `test_fernet_secret` from Airflow Variables and prints it.

    ## Use Case
    - **Before Fernet**: Check Postgres → plain text
    - **After Fernet**: Check Postgres → encrypted (`gAAAAA...`)

    ## Steps
    1. Set variable: `Variable.set("test_fernet_secret", "my-secret")`
    2. Trigger DAG
    3. Check logs + DB
    """
) as dag:

    print_task = PythonOperator(
        task_id="print_fernet_secret",
        python_callable=print_secret,
    )