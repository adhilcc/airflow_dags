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
    """GEM pipeline health report. Cheap by design: MAX(event_id) (indexed PK) for
    liveness, plus a 24h window aggregated over the indexed event_timestamp (a bounded
    range scan, NOT a full-table scan). Grouping is summed in Python so the SQL is
    dialect-agnostic. Fails only on a real connect/query error; otherwise it just prints
    the report (a flat delta is a WARN, fine when the system is idle)."""
    from datetime import datetime, timedelta, timezone
    ev = f"{GEM_SCHEMA}.airflow_events"
    md = f"{GEM_SCHEMA}.dag_metadata"
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    with _gem_engine().connect() as c:
        max_id = c.execute(text(f"SELECT MAX(event_id) FROM {ev}")).scalar() or 0
        last_ts = c.execute(text(f"SELECT MAX(event_timestamp) FROM {ev}")).scalar()
        dags_tracked = c.execute(text(f"SELECT COUNT(*) FROM {md}")).scalar()
        run_rows = c.execute(text(
            f"SELECT event_status, COUNT(*) FROM {ev} "
            "WHERE event_type='dag_run' AND event_timestamp >= :cut GROUP BY event_status"),
            {"cut": cutoff}).fetchall()
        task_failed = c.execute(text(
            f"SELECT COUNT(*) FROM {ev} WHERE event_type='task_instance' "
            "AND event_status='failed' AND event_timestamp >= :cut"), {"cut": cutoff}).scalar()
        active_dags = c.execute(text(
            f"SELECT COUNT(DISTINCT dag_id) FROM {ev} "
            "WHERE event_type='dag_run' AND event_timestamp >= :cut"), {"cut": cutoff}).scalar()
        fail_rows = c.execute(text(
            f"SELECT dag_id, COUNT(*) FROM {ev} WHERE event_type='dag_run' "
            "AND event_status='failed' AND event_timestamp >= :cut GROUP BY dag_id"),
            {"cut": cutoff}).fetchall()
    outcomes = {str(s).lower(): int(n) for s, n in run_rows}
    started = outcomes.get("running", 0)
    succeeded = outcomes.get("success", 0)
    failed = outcomes.get("failed", 0)
    completed = succeeded + failed
    rate = f"{100.0 * succeeded / completed:.1f}%" if completed else "n/a"
    top_fail = sorted(((d, int(n)) for d, n in fail_rows), key=lambda x: x[1], reverse=True)[:5]
    prev_id = int(Variable.get("GEM_PREV_MAX_EVENT_ID", default_var="0") or 0)
    Variable.set("GEM_PREV_MAX_EVENT_ID", str(max_id))
    delta = max_id - prev_id
    # The Airflow web UI strips ANSI colour codes, so we use emoji instead - browsers
    # render them as COLOUR glyphs (green check / red cross / amber warning), which is
    # how the report shows colour in the UI log. Falls back to ASCII if the runtime
    # locale can't encode emoji (so the monitor never fails on a print).
    r = ["================= GEM PIPELINE REPORT (last 24h) ================="]
    r.append(f"  \U0001F4CA liveness      : latest event_id={max_id}  (+{delta} since last check)")
    r.append(f"  \U0001F551 last event    : {last_ts}")
    r.append(f"  \U0001F4DA DAGs tracked  : {dags_tracked}   |   active (24h): {active_dags}")
    r.append(f"  ▶️  runs started  : {started}")
    r.append(f"  ✅ runs success  : {succeeded}")
    r.append(f"  ❌ runs failed   : {failed}")
    r.append(f"  \U0001F4C8 success rate  : {rate}  ({succeeded}/{completed} completed)")
    r.append(f"  ⚠️  task failures : {task_failed}")
    if top_fail:
        r.append("  \U0001F53B top failing DAGs (24h):")
        for d, n in top_fail:
            r.append(f"        - {d}: {n}")
    r.append("==================================================================")
    if delta > 0:
        r.append("✅ OK: new events since last check - GEM pipeline is writing.")
    else:
        r.append("⚠️  WARN: no new events since last check (ok if idle; check if last event is stale).")
    msg = "\n".join(r)
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode("ascii", "replace").decode("ascii"))


with DAG(
    dag_id="gem_row_count_check",
    start_date=datetime(2024, 1, 1, tzinfo=pendulum.timezone("UTC")),
    schedule_interval="*/10 * * * *",   # every 10 min; also trigger manually
    catchup=False,
    tags=["GEM", "MONITOR"],
    description="Counts the two GEM tables and logs growth vs the previous run.",
) as dag:
    PythonOperator(task_id="gem_row_counts", python_callable=gem_row_counts)
