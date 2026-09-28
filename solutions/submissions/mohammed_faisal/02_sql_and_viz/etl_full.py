"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
Solution File: etl_full.py
Pillar: SQL & Visualization (Task 2.2)
Author: Mohammed Faisal
=============================================================

Full ETL: projects + employees (01_foundations/etl_pipeline.py) + transactions, enriched, checked
with the DQ framework (04_infrastructure/dq_framework.py) and written to
outputs/results/mohammed_faisal/02_sql_and_viz/. The Docker image and the Airflow DAG run this code.

HOW TO RUN
----------
  python solutions/submissions/mohammed_faisal/02_sql_and_viz/etl_full.py
"""

import json
import os
import sys
import time

import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
# Make the sibling pillar folders importable (01_foundations, 04_infrastructure),
# whether the script runs from the repo, inside Docker or inside Airflow.
for _p in (os.environ.get("PRESIGHT_ETL_DIR"), os.path.join(_HERE, "..", "01_foundations"),
           os.path.join(_HERE, "..", "04_infrastructure"), _HERE):
    if _p:
        sys.path.insert(0, _p)
import etl_pipeline as base  # noqa: E402
# Re-export the Pillar 1 functions so callers (Airflow DAG, tests) need only this module.
from etl_pipeline import (DATA_DIR, RUN_STATS, _decision, _record_count, clean_employees,  # noqa: E402,F401
                          load_employees, load_projects, logger, transform_projects, write_outputs)
import dq_framework as dq  # noqa: E402

# Output folder: env OUTPUT_DIR, the Airflow mount, or outputs/results/<me>/02_sql_and_viz.
OUTPUT_DIR = base.default_output_dir("02_sql_and_viz")


def load_transactions(filepath: str) -> pd.DataFrame:
    """
    Load transactions.json (array of flat objects) into a typed DataFrame.

    Null-handling decisions
    -----------------------
    * amount null (~1.5%): kept as NULL here; enrich_transactions() adds amount_aed
      with 0.0 as required. We do not impute a value: guessing spend would
      distort finance totals.
    * approved_by null (~5%): kept as NULL -> is_approved = False (meaningful state).
    * notes null: kept (free text).
    """
    logger.info("Loading transactions data...")
    # The file is one JSON array of flat records; json_normalize turns it into a table.
    with open(filepath, "r", encoding="utf-8") as fh:
        records = json.load(fh)
    df = pd.json_normalize(records)          # flattens (also copes with nested objects)
    _record_count("transactions", raw=len(df))

    # Explicit types: unparseable dates/amounts become NaT/NaN instead of raising.
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    for col in ["transaction_id", "project_id", "vendor_id", "vendor_name", "category", "currency",
                "approved_by", "payment_status", "invoice_ref", "notes"]:
        df[col] = df[col].astype("string")

    logger.info("Transactions: %d rows | null amount=%d | null approved_by=%d | bad dates=%d",
                len(df), df["amount"].isna().sum(), df["approved_by"].isna().sum(),
                df["transaction_date"].isna().sum())
    _decision("transactions: null amount kept NULL in `amount`, 0.0 in `amount_aed`; "
              "null approved_by kept NULL and exposed as is_approved=False.")
    return df


def enrich_transactions(
    transactions: pd.DataFrame,
    projects: pd.DataFrame,
    employees: pd.DataFrame
) -> pd.DataFrame:
    """
    Left-join project and approver context. Lookup tables are reduced to the
    needed columns and de-duplicated on their key first, so a join can never
    multiply rows (asserted at the end).
    """
    logger.info("Enriching transactions...")
    n_before = len(transactions)

    # Keep only the needed columns and one row per key, so a join can never duplicate rows.
    proj_lookup = projects[["project_id", "project_name", "department"]].drop_duplicates("project_id")
    emp_lookup = employees[["employee_id", "full_name"]].drop_duplicates("employee_id").rename(
        columns={"employee_id": "approved_by", "full_name": "approver_name"}
    )

    df = (
        transactions
        # Left joins keep every transaction even when the project or approver is unknown.
        .merge(proj_lookup, on="project_id", how="left")
        .merge(emp_lookup, on="approved_by", how="left")
    )
    # Derived columns required by Task 2.2.
    df["is_approved"] = df["approved_by"].notna()
    df["amount_aed"] = df["amount"].astype("float64").fillna(0.0)
    df["transaction_year_month"] = df["transaction_date"].dt.strftime("%Y-%m")

    # Safety net: a join on a duplicated key would silently multiply rows.
    assert len(df) == n_before, "Join produced row duplication!"
    unmatched = df["project_name"].isna().sum()
    if unmatched:
        logger.warning("%d transactions reference a project not in projects.csv", unmatched)
    _record_count("transactions", clean=len(df))
    return df


# Orchestrates Task 2.2 end to end: load, clean, enrich, quality-check, write.
def run_pipeline():
    """Load -> clean -> enrich -> DQ checks -> write outputs."""
    # Start of the timed section; the duration goes into pipeline_summary.txt.
    t0 = time.time()
    logger.info("Starting ETL pipeline (DATA_DIR=%s, OUTPUT_DIR=%s)", DATA_DIR, OUTPUT_DIR)

    projects = transform_projects(load_projects(os.path.join(DATA_DIR, "projects.csv")))
    # Salary history is used to recover missing hire dates for employees.
    history = pd.read_csv(os.path.join(DATA_DIR, "employees_salary_history.csv"))
    employees = clean_employees(load_employees(os.path.join(DATA_DIR, "employees.csv")), history)
    transactions = enrich_transactions(load_transactions(os.path.join(DATA_DIR, "transactions.json")),
                                       projects, employees)

    # Run the DQ framework on every dataset; the reference tables feed the foreign-key checks.
    refs = {"projects": projects, "employees": employees}
    for name, df in [("projects", projects), ("employees", employees),
                     ("transactions", transactions), ("employees_salary_history", history)]:
        # Run all checks for one dataset and log a one-line summary.
        res = dq.run_data_quality_checks(df, name, reference_tables=refs)
        logger.info("DQ %-26s run=%d passed=%d failed=%d", name, res["checks_run"],
                    res["checks_passed"], res["checks_failed"])
        dq.write_dq_report(res, OUTPUT_DIR)

    # Write the cleaned CSVs plus pipeline_summary.txt (also employees_quality_summary.json).
    write_outputs({"projects_clean": projects, "employees_clean": employees,
                   "transactions_clean": transactions}, OUTPUT_DIR, time.time() - t0)
    logger.info("Pipeline complete in %.2fs. Outputs written to: %s", time.time() - t0, OUTPUT_DIR)


if __name__ == "__main__":
    run_pipeline()
