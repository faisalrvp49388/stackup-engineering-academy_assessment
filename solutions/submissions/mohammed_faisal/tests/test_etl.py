"""Unit tests for the ETL + DQ framework (run: pytest -q)."""
import os
import sys

import pandas as pd
import pytest

# Make the solution modules importable when pytest runs from the repo root.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "02_sql_and_viz"))
import etl_full as etl  # noqa: E402  (adds 01_foundations and 04_infrastructure to sys.path)
import etl_pipeline as base  # noqa: E402
import dq_framework as dq  # noqa: E402

DATA = base.DATA_DIR


# Task 1.1: derived columns, status categories, null handling and the risk rules.
def test_projects_derived_columns_and_risk():
    df = etl.transform_projects(etl.load_projects(os.path.join(DATA, "projects.csv")))
    assert len(df) == 500
    assert set(df["status_category"]) <= {"Active", "Closed", "Pending"}
    assert df["budget"].notna().all() and df["actual_cost"].notna().all()
    over = df["is_over_budget"]
    assert (df.loc[over, "risk_level"] == "High").all()
    assert (df.loc[df["priority"] == "Critical", "risk_level"] == "High").all()
    ok = df["duration_days"].dropna()
    assert (ok >= 0).all()


# Task 1.3: every known issue in employees.csv is repaired.
def test_employee_cleaning_fixes_known_issues():
    raw = etl.load_employees(os.path.join(DATA, "employees.csv"))
    hist = pd.read_csv(os.path.join(DATA, "employees_salary_history.csv"))
    clean = etl.clean_employees(raw, hist)
    assert len(clean) == 1000
    assert clean["email"].notna().all() and clean["email"].is_unique
    assert (clean["years_experience"] >= 0).all()
    lo = clean["level"].map({k: v[0] for k, v in base.LEVEL_SALARY_BANDS.items()})
    hi = clean["level"].map({k: v[1] for k, v in base.LEVEL_SALARY_BANDS.items()})
    assert clean["salary"].between(lo, hi).all()
    assert pd.api.types.is_datetime64_any_dtype(clean["hire_date"])


# Task 2.2: joins keep exactly one row per transaction and null amounts become 0.0.
def test_enrichment_does_not_duplicate_rows():
    p = etl.transform_projects(etl.load_projects(os.path.join(DATA, "projects.csv")))
    e = etl.clean_employees(etl.load_employees(os.path.join(DATA, "employees.csv")))
    t = etl.load_transactions(os.path.join(DATA, "transactions.json"))
    out = etl.enrich_transactions(t, p, e)
    assert len(out) == len(t) == 50000
    assert (out.loc[out["amount"].isna(), "amount_aed"] == 0).all()


# Task 4.3: result format matches the brief and behaviour is driven purely by config.
def test_dq_result_format_and_config_driven():
    df = pd.DataFrame({"id": [1, 2, 2], "v": [1, -5, 999]})
    cfg = {"t": {"completeness_threshold": 0.9, "pk_columns": ["id"],
                 "numeric_ranges": {"v": {"min": 0, "max": 100}}}}
    res = dq.run_data_quality_checks(df, "t", config=cfg)
    assert res["dataset_name"] == "t"
    assert res["checks_run"] == res["checks_passed"] + res["checks_failed"]
    assert res["results"]["uniqueness"]["status"] == "FAIL"
    assert res["results"]["validity_numeric"]["status"] == "FAIL"
    assert res["results"]["completeness"]["status"] == "PASS"


# Airflow gate: incomplete key columns must raise ValueError.
def test_dq_gate_blocks_incomplete_data():
    df = pd.DataFrame({"project_id": ["a", "b", "c", "d", "e"], "budget": [1, None, None, None, None]})
    cfg = {"projects": {"completeness_threshold": 0.9}}
    with pytest.raises(ValueError, match="Data quality gate FAILED"):
        dq.enforce_dq_gate({"projects": df}, config=cfg)


# Airflow gate: the real datasets must pass so the DAG can proceed.
def test_dq_gate_passes_on_real_data():
    datasets = {
        "projects": etl.load_projects(os.path.join(DATA, "projects.csv")),
        "employees": etl.load_employees(os.path.join(DATA, "employees.csv")),
        "transactions": etl.load_transactions(os.path.join(DATA, "transactions.json")),
    }
    assert set(dq.enforce_dq_gate(datasets)) == set(datasets)


# Pillar 1 deliverables: all four files are written and the quality summary is correct.
def test_foundations_outputs(tmp_path):
    written = base.run_foundations(str(tmp_path))
    assert set(written) == {"projects_clean", "employees_clean", "employees_quality_summary", "pipeline_summary"}
    import json
    summary = json.load(open(written["employees_quality_summary"], encoding="utf-8"))
    assert summary["rows_before"] == summary["rows_after"] == 1000
    assert summary["issues_detected_rows"]["Invalid date formats"] == 8
    assert summary["rows_affected_per_fix_type"]["missing email -> derived from name"] == 10
