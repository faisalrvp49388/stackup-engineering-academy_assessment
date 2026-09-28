"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
File: airflow_dag.py  (solution)
Author: Mohammed Faisal
Pillar: Big Data Processing — Task 3.3
=============================================================

DAG: presight_etl_pipeline — daily at 06:00 Asia/Dubai (02:00 UTC)

  start
    ├── extract_projects ───┐
    ├── extract_employees ──┼──► validate_data_quality ──► transform_and_enrich
    └── extract_transactions┘        (DQ gate)                     │
                                                                   ▼
                                     end ◄── generate_pipeline_report ◄── load_to_output

The DQ gate raises ValueError when a key column is < 80% complete; because all later
tasks use the default `all_success` trigger rule they are then marked upstream_failed
and never execute.

Between tasks only small values go through XCom (row counts, DQ summary, file list).
The cleaned DataFrames are handed from transform -> load through a staging directory
(tasks may run in different processes, so DataFrames cannot live in memory).

HOW TO RUN
----------
  docker compose up -d           # Airflow UI on http://localhost:8081  (admin / admin)
  docker compose up -d mounts this folder (see docker-compose.override.yml); then
  airflow dags trigger presight_etl_pipeline
"""

import logging
import os
import pickle
import sys
from datetime import timedelta

import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Shared modules live in the sibling pillar folders; inside Airflow the whole solution folder is
# mounted at PRESIGHT_SOLUTION_DIR (see docker-compose.override.yml).
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.environ.get("PRESIGHT_SOLUTION_DIR", os.path.join(_HERE, "..")), "02_sql_and_viz"))
import etl_full as etl  # noqa: E402  (also puts 01_foundations and 04_infrastructure on sys.path)
import dq_framework as dq  # noqa: E402

logger = logging.getLogger(__name__)

DATA_DIR = etl.DATA_DIR
OUTPUT_DIR = etl.OUTPUT_DIR
# Scratch folder used to pass the cleaned DataFrames from transform to load.
STAGING_DIR = os.path.join(OUTPUT_DIR, ".staging")
# The schedule and start date are interpreted in UAE time (UTC+4).
UAE_TZ = pendulum.timezone("Asia/Dubai")


# ==============================================================================
# TASK 3.3a — DAG default arguments
# ==============================================================================

# Failure alert callback: logs which task failed (swap for email/Slack in production).
def notify_failure(context):
    """Failure alert (log based; swap for email/Slack in production)."""
    ti = context["task_instance"]
    logger.error("ALERT: task %s of DAG %s failed (try %s) — %s",
                 ti.task_id, ti.dag_id, ti.try_number, context.get("exception"))


# Defaults applied to every task (owner, start date in Asia/Dubai, retries, no email).
default_args = {
    "owner": "mohammed.faisal",
    "start_date": pendulum.datetime(2025, 1, 1, tz=UAE_TZ),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,          # set True + "email": [...] when SMTP is configured
    "email_on_retry": False,
    "depends_on_past": False,
    "on_failure_callback": notify_failure,
}


# ==============================================================================
# TASK 3.3b — Task functions
# ==============================================================================

# Absolute path of a raw dataset file.
def _path(name: str) -> str:
    return os.path.join(DATA_DIR, name)


# Extract tasks only load the data and publish the raw row count through XCom.
def task_extract_projects(**context):
    ti = context["ti"]
    df = etl.load_projects(_path("projects.csv"))
    # XCom carries only small values (counts, summaries, file lists) between tasks.
    ti.xcom_push(key="projects_raw_count", value=int(len(df)))
    return f"Extracted {len(df)} projects"


def task_extract_employees(**context):
    ti = context["ti"]
    df = etl.load_employees(_path("employees.csv"))
    ti.xcom_push(key="employees_raw_count", value=int(len(df)))
    return f"Extracted {len(df)} employees"


def task_extract_transactions(**context):
    ti = context["ti"]
    df = etl.load_transactions(_path("transactions.json"))
    ti.xcom_push(key="transactions_raw_count", value=int(len(df)))
    return f"Extracted {len(df)} transactions"


# DQ gate: any critical failure raises ValueError, which stops all downstream tasks.
def task_validate_data_quality(**context):
    """
    DQ GATE. Reloads the three datasets, runs the configurable DQ framework and raises
    ValueError when a key column is less than 80% complete (see dq.enforce_dq_gate).
    """
    ti = context["ti"]
    datasets = {
        "projects": etl.load_projects(_path("projects.csv")),
        "employees": etl.load_employees(_path("employees.csv")),
        "transactions": etl.load_transactions(_path("transactions.json")),
    }
    results = dq.enforce_dq_gate(datasets)      # raises ValueError -> task fails -> DAG stops

    # Compact DQ summary that is safe to store in XCom.
    summary = {name: {"checks_run": r["checks_run"], "passed": r["checks_passed"],
                      "failed": r["checks_failed"],
                      "failed_checks": [c for c, v in r["results"].items() if v["status"] == "FAIL"]}
               for name, r in results.items()}
    ti.xcom_push(key="dq_results", value=summary)
    logger.info("DQ gate passed: %s", summary)
    return "DQ gate passed"


# Transform: clean each dataset and stage the results on disk for the next task.
def task_transform_and_enrich(**context):
    ti = context["ti"]
    projects = etl.transform_projects(etl.load_projects(_path("projects.csv")))
    history_path = _path("employees_salary_history.csv")
    history = pd.read_csv(history_path) if os.path.exists(history_path) else None
    employees = etl.clean_employees(etl.load_employees(_path("employees.csv")), history)
    transactions = etl.enrich_transactions(etl.load_transactions(_path("transactions.json")),
                                           projects, employees)

    os.makedirs(STAGING_DIR, exist_ok=True)
    # Tasks run in separate processes, so DataFrames are handed over via a staging file.
    with open(os.path.join(STAGING_DIR, "clean.pkl"), "wb") as fh:
        pickle.dump({"projects": projects, "employees": employees, "transactions": transactions,
                     "run_stats": etl.RUN_STATS}, fh)

    ti.xcom_push(key="projects_clean_count", value=int(len(projects)))
    ti.xcom_push(key="employees_clean_count", value=int(len(employees)))
    ti.xcom_push(key="transactions_clean_count", value=int(len(transactions)))
    return "Transformed"


# Load: write the staged DataFrames to the output folder and publish the file list.
def task_load_to_output(**context):
    ti = context["ti"]
    with open(os.path.join(STAGING_DIR, "clean.pkl"), "rb") as fh:
        staged = pickle.load(fh)
    etl.RUN_STATS.update(staged["run_stats"])       # keep quality notes for pipeline_summary.txt
    written = etl.write_outputs({"projects_clean": staged["projects"], "employees_clean": staged["employees"],
                                 "transactions_clean": staged["transactions"]}, OUTPUT_DIR)
    for name, path in written.items():
        logger.info("Wrote %s -> %s", name, path)
    ti.xcom_push(key="files_written", value=list(written.values()))
    return f"Wrote {len(written)} files"


# Report: pull the XCom values from upstream tasks and write pipeline_report_<date>.txt.
def task_generate_pipeline_report(**context):
    ti = context["ti"]
    execution_date = context.get("logical_date") or context["execution_date"]

    # Helper that reads an XCom value pushed by an upstream task.
    pull = lambda task, key: ti.xcom_pull(task_ids=task, key=key)   # noqa: E731
    datasets = {
        "projects": ("extract_projects", "projects_raw_count", "projects_clean_count"),
        "employees": ("extract_employees", "employees_raw_count", "employees_clean_count"),
        "transactions": ("extract_transactions", "transactions_raw_count", "transactions_clean_count"),
    }
    dq = pull("validate_data_quality", "dq_results") or {}
    files = pull("load_to_output", "files_written") or []

    lines = [
        "PRESIGHT ETL PIPELINE REPORT",
        "=" * 60,
        f"DAG run (logical) date : {execution_date}",
        f"Generated at           : {pendulum.now('Asia/Dubai').isoformat()}",
        "",
        "ROW COUNTS (raw -> clean)",
        "-" * 60,
    ]
    for name, (ext_task, raw_key, clean_key) in datasets.items():
        lines.append(f"  {name:<14} raw={pull(ext_task, raw_key)}   clean={pull('transform_and_enrich', clean_key)}")
    lines += ["", "DATA QUALITY RESULTS", "-" * 60]
    for name, r in dq.items():
        lines.append(f"  {name:<14} run={r['checks_run']} passed={r['passed']} failed={r['failed']}"
                     + (f"  ({', '.join(r['failed_checks'])})" if r["failed_checks"] else ""))
    lines += ["", "FILES WRITTEN", "-" * 60] + [f"  {f}" for f in files]

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    report_path = os.path.join(OUTPUT_DIR, f"pipeline_report_{execution_date.date()}.txt")
    with open(report_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    logger.info("Pipeline report written to %s", report_path)
    return report_path


# ==============================================================================
# TASK 3.3a — DAG definition
# ==============================================================================

# DAG definition: daily 06:00 Asia/Dubai, no catch-up, one active run at a time.
with DAG(
    dag_id="presight_etl_pipeline",
    default_args=default_args,
    description="Daily ETL pipeline for Presight project management data",
    schedule="0 6 * * *",                 # 06:00 in the DAG timezone (Asia/Dubai = UTC+4 -> 02:00 UTC)
    start_date=pendulum.datetime(2025, 1, 1, tz=UAE_TZ),
    catchup=False,
    tags=["presight", "etl", "assessment"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # One PythonOperator per task; the callables are defined above.
    extract_projects = PythonOperator(task_id="extract_projects", python_callable=task_extract_projects)
    extract_employees = PythonOperator(task_id="extract_employees", python_callable=task_extract_employees)
    extract_transactions = PythonOperator(task_id="extract_transactions",
                                          python_callable=task_extract_transactions)

    validate_dq = PythonOperator(task_id="validate_data_quality", python_callable=task_validate_data_quality)

    transform_enrich = PythonOperator(task_id="transform_and_enrich", python_callable=task_transform_and_enrich)
    load_output = PythonOperator(task_id="load_to_output", python_callable=task_load_to_output)
    pipeline_report = PythonOperator(task_id="generate_pipeline_report",
                                     python_callable=task_generate_pipeline_report)

    # ===========================================================================
    # TASK 3.3c — dependencies: parallel extracts -> DQ gate -> transform -> load -> report
    # ===========================================================================
    # Dependencies: three parallel extracts -> DQ gate -> transform -> load -> report.
    start >> [extract_projects, extract_employees, extract_transactions] >> validate_dq
    validate_dq >> transform_enrich >> load_output >> pipeline_report >> end
