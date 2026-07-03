# gem_row_count_check_dag.py
#
# LOCAL sample DAG to verify the GEM pipeline is loading data, WITHOUT any DB shell
# access: it counts the two GEM tables and compares each run to the previous one
# (persisted in Airflow Variables), so you can watch the counts grow.
#
# Reads the GEM database from the SAME Airflow UI connection the consumer uses
# (gem_event_listener). Works for a Postgres connection (local) and an mssql one
# (TCW) — only GEM_SCHEMA differs (local Postgres = schema_airflow; TCW = dbo).
#
# It FAILS only on a real connect/query error (the thing you want to catch). A flat
# count just logs a WARN (normal when no other DAGs ran between checks).

from datetime import datetime
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

GEM_CONN_ID = "gem_event_listener"
GEM_SCHEMA = "schema_airflow"   # local Postgres default schema; set "dbo" for SQL Server


def _odbc_val(v):
    return "{" + str(v).replace("}", "}}") + "}"


def _gem_engine():
    """Build a SQLAlchemy engine from the Airflow UI connection (no creds in the DAG)."""
    conn = BaseHook.get_connection(GEM_CONN_ID)
    ct = (conn.conn_type or "").lower()
    if ct in ("mssql", "odbc", "sqlserver"):
        driver = (conn.extra_dejson or {}).get("driver") or "ODBC Driver 17 for SQL Server"
        server = f"{conn.host},{conn.port}" if conn.port else conn.host
        odbc = (
            f"DRIVER={{{driver}}};SERVER={server};DATABASE={conn.schema};"
            f"UID={_odbc_val(conn.login)};PWD={_odbc_val(conn.password)};"
            f"Encrypt=yes;TrustServerCertificate=no;"
        )
        return create_engine("mssql+pyodbc:///?odbc_connect=" + quote_plus(odbc), pool_pre_ping=True)
    pw = quote_plus(conn.password or "")
    port = conn.port or 5432
    return create_engine(
        f"postgresql+psycopg2://{conn.login}:{pw}@{conn.host}:{port}/{conn.schema}",
        pool_pre_ping=True,
    )


def gem_row_counts(**_):
    with _gem_engine().connect() as c:
        ev = c.execute(text(f"SELECT COUNT(*) FROM {GEM_SCHEMA}.airflow_events")).scalar()
        md = c.execute(text(f"SELECT COUNT(*) FROM {GEM_SCHEMA}.dag_metadata")).scalar()
        last_ing = c.execute(text(f"SELECT MAX(ingested_at) FROM {GEM_SCHEMA}.airflow_events")).scalar()
    prev_ev = int(Variable.get("GEM_PREV_EVENTS_COUNT", default_var="0") or 0)
    prev_md = int(Variable.get("GEM_PREV_METADATA_COUNT", default_var="0") or 0)
    Variable.set("GEM_PREV_EVENTS_COUNT", str(ev))
    Variable.set("GEM_PREV_METADATA_COUNT", str(md))
    print(f"airflow_events: {ev} rows (prev {prev_ev}, delta +{ev - prev_ev}); last ingested_at = {last_ing}")
    print(f"dag_metadata:   {md} rows (prev {prev_md}, delta +{md - prev_md})")
    if ev - prev_ev > 0:
        print("OK: airflow_events grew since last check - GEM pipeline is writing.")
    else:
        print("WARN: airflow_events flat since last check (ok if no DAG runs occurred; "
              "investigate if this persists or last ingested_at is stale).")


with DAG(
    dag_id="gem_row_count_check",
    start_date=datetime(2024, 1, 1, tzinfo=pendulum.timezone("UTC")),
    schedule_interval="*/10 * * * *",   # every 10 min; also trigger manually
    catchup=False,
    tags=["GEM", "MONITOR"],
    description="Counts the two GEM tables and logs growth vs the previous run.",
) as dag:
    PythonOperator(task_id="gem_row_counts", python_callable=gem_row_counts)
