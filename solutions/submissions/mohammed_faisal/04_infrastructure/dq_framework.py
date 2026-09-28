"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
Solution File: dq_framework.py
Pillar: Infrastructure & Governance (Task 4.3)
Author: Mohammed Faisal
=============================================================

Configurable data-quality framework. Rules live in DQ_CONFIG (or a YAML file with the same
structure); every check is a function registered in DQ_CHECKS, so adding a rule or a check never
touches pipeline code. Used by 02_sql_and_viz/etl_full.py and the Airflow DQ gate.

HOW TO RUN (standalone, regenerates dq_report_<dataset>.md)
----------
  python solutions/submissions/mohammed_faisal/04_infrastructure/dq_framework.py [--config rules.yaml]
"""

import argparse
import logging
import os
import sys
from datetime import datetime

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "01_foundations"))
from etl_pipeline import LEVEL_SALARY_BANDS, default_output_dir  # noqa: E402

logger = logging.getLogger(__name__)
OUTPUT_DIR = default_output_dir("04_infrastructure")

# Rules live in DQ_CONFIG (or an external YAML passed to load_dq_config()); adding
# a rule means editing config only. Each check is a registered function taking
# (df, cfg, context) and returning (passed: bool, details, failed_columns).

DQ_CONFIG = {
    # Rules for projects (thresholds, keys, ranges, date order, foreign keys, bonus checks).
    "projects": {
        "completeness_threshold": 0.90,
        # end_date is only populated for Completed projects; derived columns inherit nulls
        "completeness_optional": ["end_date", "duration_days", "budget_variance", "budget_utilisation_pct"],
        "pk_columns": ["project_id"],
        "numeric_ranges": {
            "budget": {"min": 0, "max": 10_000_000},
            "actual_cost": {"min": 0, "max": 10_000_000},
        },
        "date_columns": {"start_date": {"allow_future": True}, "end_date": {"allow_future": True}},
        "consistency_rules": [
            {"type": "before", "columns": ["start_date", "end_date"]},
            {"type": "non_negative", "column": "actual_cost"},
        ],
        "foreign_keys": {"project_manager_id": ("employees", "employee_id")},
        # Bonus checks
        "distribution_max_share": {"threshold": 0.30, "columns": ["project_manager_id", "department"]},
        "outlier_zscore": {"threshold": 3, "columns": ["budget", "actual_cost"]},
    },
    # Rules for employees, including the salary-vs-level consistency rule.
    "employees": {
        "completeness_threshold": 0.85,
        "completeness_optional": ["dq_flags", "manager_id"],   # audit column / top of hierarchy
        "pk_columns": ["employee_id"],
        "numeric_ranges": {
            "salary": {"min": 10_000, "max": 100_000},
            "years_experience": {"min": 0, "max": 50},
        },
        "date_columns": {"hire_date": {"allow_future": False}},
        "consistency_rules": [
            {"type": "non_negative", "column": "salary"},
            {"type": "level_salary_band", "level_column": "level", "salary_column": "salary",
             "bands": LEVEL_SALARY_BANDS},
        ],
        "foreign_keys": {"manager_id": ("employees", "employee_id")},
        "distribution_max_share": {"threshold": 0.30, "columns": ["department", "role"]},
        "outlier_zscore": {"threshold": 3, "columns": ["salary"]},
    },
    # Rules for transactions, including freshness and foreign keys to projects/employees.
    "transactions": {
        "completeness_threshold": 0.90,
        "completeness_optional": ["notes", "approver_name"],   # free text / unapproved rows
        "pk_columns": ["transaction_id"],
        "numeric_ranges": {"amount": {"min": 0, "max": 5_000_000}},
        "date_columns": {"transaction_date": {"allow_future": False}},
        "consistency_rules": [{"type": "non_negative", "column": "amount"}],
        "foreign_keys": {
            "project_id": ("projects", "project_id"),
            "approved_by": ("employees", "employee_id"),
        },
        "distribution_max_share": {"threshold": 0.30, "columns": ["vendor_id", "category", "project_id"]},
        "freshness": {"date_column": "transaction_date", "max_age_days": 30},
        "outlier_zscore": {"threshold": 3, "columns": ["amount"]},
    },
    # Rules for the salary history (previous_* columns are legitimately empty on Hire rows).
    "employees_salary_history": {
        "completeness_threshold": 0.90,
        "completeness_optional": ["previous_salary", "previous_role", "previous_level"],  # null on 'Hire' rows
        "pk_columns": ["employee_id", "effective_date"],
        "numeric_ranges": {"new_salary": {"min": 10_000, "max": 100_000}},
        "date_columns": {"effective_date": {"allow_future": False}},
        "consistency_rules": [{"type": "non_negative", "column": "new_salary"}],
        "foreign_keys": {"employee_id": ("employees", "employee_id")},
    },
}

# Failures on these checks are "critical" for the Airflow DQ gate.
CRITICAL_COMPLETENESS = 0.80


def load_dq_config(path: str = None) -> dict:
    """Return DQ_CONFIG, or the YAML file at `path` (same structure) if supplied."""
    if not path:
        return DQ_CONFIG
    import yaml
    with open(path, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    for ds in cfg.values():   # YAML has no tuples
        ds["foreign_keys"] = {k: tuple(v) for k, v in ds.get("foreign_keys", {}).items()}
    return cfg


# ---- individual checks ---------------------------------------------------------------
# Completeness (column level): share of non-null, non-empty values per column.
def _check_completeness(df, cfg, ctx):
    threshold = cfg.get("completeness_threshold", 0.9)
    blank = df.isna() | (df.astype("string") == "").fillna(False)
    ratios = (1 - blank.mean()).round(4)
    optional = set(cfg.get("completeness_optional", []))
    failed = [c for c, v in ratios.items() if v < threshold and c not in optional]
    return not failed, ratios.to_dict(), failed


# Uniqueness (table level): primary-key columns must not contain duplicates.
def _check_uniqueness(df, cfg, ctx):
    pk = cfg.get("pk_columns")
    if not pk:
        return True, "no pk configured", []
    dups = int(df.duplicated(subset=pk).sum())
    return dups == 0, f"{'+'.join(pk)}: {len(df) - dups} unique / {len(df)} total ({dups} duplicates)", (pk if dups else [])


# Validity, numeric: values must lie within the configured min/max.
def _check_validity_numeric(df, cfg, ctx):
    msgs, failed = [], []
    for col, rng in cfg.get("numeric_ranges", {}).items():
        if col not in df:
            continue
        s = pd.to_numeric(df[col], errors="coerce")
        low, high = int((s < rng["min"]).sum()), int((s > rng["max"]).sum())
        if low or high:
            failed.append(col)
            msgs.append(f"{col}: {low} below min ({rng['min']}), {high} above max ({rng['max']})")
    return not failed, "; ".join(msgs) or "all values within range", failed


# Validity, date: values must parse, and must not be in the future when configured.
def _check_validity_date(df, cfg, ctx):
    msgs, failed = [], []
    now = pd.Timestamp.today().normalize()
    for col, opts in cfg.get("date_columns", {}).items():
        if col not in df:
            continue
        raw = df[col]
        parsed = pd.to_datetime(raw, errors="coerce")
        invalid = int((raw.notna() & parsed.isna()).sum())
        future = int((parsed > now).sum()) if not opts.get("allow_future", True) else 0
        if invalid or future:
            failed.append(col)
            msgs.append(f"{col}: {invalid} unparseable, {future} in the future")
    return not failed, "; ".join(msgs) or "all dates valid", failed


# Consistency (cross column): date order, non-negative values, salary vs level band.
def _check_consistency(df, cfg, ctx):
    msgs, failed = [], []
    for rule in cfg.get("consistency_rules", []):
        if rule["type"] == "before":
            a, b = rule["columns"]
            both = df[a].notna() & df[b].notna()
            bad = int((pd.to_datetime(df[a])[both] >= pd.to_datetime(df[b])[both]).sum())
            label, cols = f"{a} < {b}", [a, b]
        elif rule["type"] == "non_negative":
            bad = int((pd.to_numeric(df[rule["column"]], errors="coerce") < 0).sum())
            label, cols = f"{rule['column']} >= 0", [rule["column"]]
        elif rule["type"] == "level_salary_band":
            lo = df[rule["level_column"]].map({k: v[0] for k, v in rule["bands"].items()})
            hi = df[rule["level_column"]].map({k: v[1] for k, v in rule["bands"].items()})
            s = df[rule["salary_column"]]
            bad = int(((s < lo) | (s > hi)).sum())
            label, cols = "salary within level band", [rule["salary_column"]]
        else:
            msgs.append(f"unknown rule type {rule['type']}")
            continue
        if bad:
            failed += cols
            msgs.append(f"{label}: {bad} violations")
    return not msgs, "; ".join(msgs) or "all consistency rules hold", sorted(set(failed))


# Referential integrity (cross table): foreign keys must exist in the referenced table.
def _check_referential(df, cfg, ctx):
    msgs, failed = [], []
    for fk, (ref_table, ref_col) in cfg.get("foreign_keys", {}).items():
        if fk not in df:
            continue
        ref_df = ctx.get(ref_table)
        if ref_df is None:
            msgs.append(f"{fk}: reference table '{ref_table}' unavailable (skipped)")
            continue
        vals = df[fk].dropna()
        orphans = int((~vals.isin(ref_df[ref_col])).sum())
        if orphans:
            failed.append(fk)
            msgs.append(f"{fk}: {orphans} values missing in {ref_table}.{ref_col}")
    return not failed, "; ".join(msgs) or "all foreign keys resolve", failed


# Bonus: flag a column where one value holds more than the allowed share.
def _check_distribution(df, cfg, ctx):
    spec = cfg.get("distribution_max_share")
    if not spec:
        return None
    failed, msgs = [], []
    for col in spec["columns"]:
        if col in df and df[col].notna().any():
            top = df[col].value_counts(normalize=True, dropna=True)
            if top.iloc[0] > spec["threshold"]:
                failed.append(col)
                msgs.append(f"{col}: '{top.index[0]}' = {top.iloc[0]:.0%}")
    return not failed, "; ".join(msgs) or f"no column exceeds {spec['threshold']:.0%} share", failed


# Bonus: flag data whose newest date is older than the allowed age.
def _check_freshness(df, cfg, ctx):
    spec = cfg.get("freshness")
    if not spec:
        return None
    latest = pd.to_datetime(df[spec["date_column"]], errors="coerce").max()
    age = (pd.Timestamp.today().normalize() - latest).days
    return age <= spec["max_age_days"], f"latest {spec['date_column']} = {latest.date()} ({age} days old)", (
        [spec["date_column"]] if age > spec["max_age_days"] else [])


# Bonus: flag values more than N standard deviations from the mean.
def _check_outliers(df, cfg, ctx):
    spec = cfg.get("outlier_zscore")
    if not spec:
        return None
    failed, msgs = [], []
    for col in spec["columns"]:
        s = pd.to_numeric(df[col], errors="coerce")
        z = (s - s.mean()) / s.std(ddof=0)
        n = int((z.abs() > spec["threshold"]).sum())
        if n:
            failed.append(col)
            msgs.append(f"{col}: {n} values beyond {spec['threshold']} std dev")
    return not failed, "; ".join(msgs) or "no outliers", failed


# Registry: add a new check by adding one line — the runner needs no change.
# Registry: adding a new check means adding one line here.
DQ_CHECKS = {
    "completeness": _check_completeness,
    "uniqueness": _check_uniqueness,
    "validity_numeric": _check_validity_numeric,
    "validity_date": _check_validity_date,
    "consistency": _check_consistency,
    "referential_integrity": _check_referential,
    "distribution": _check_distribution,
    "freshness": _check_freshness,
    "outliers": _check_outliers,
}


# Runs every registered check that is configured for the dataset and logs a WARNING per FAIL.
def run_data_quality_checks(df: pd.DataFrame, dataset_name: str, config: dict = None,
                            reference_tables: dict = None) -> dict:
    """
    Run every registered check configured for `dataset_name`.

    reference_tables: {"employees": df, "projects": df, ...} used by the
    referential-integrity check.
    Returns the dict format from the brief; WARNING logged for each FAIL.
    """
    logger.info("Running data quality checks on: %s", dataset_name)
    cfg = (config or DQ_CONFIG).get(dataset_name, {})
    # Reference tables (e.g. employees) that the foreign-key check looks values up in.
    ctx = dict(reference_tables or {})
    ctx.setdefault(dataset_name, df)

    results, passed, failed_n = {}, 0, 0
    # Run each registered check; a check returns None when it is not configured for this dataset.
    for name, fn in DQ_CHECKS.items():
        outcome = fn(df, cfg, ctx)
        # Skip checks that have no configuration for this dataset.
        if outcome is None:          # check not configured for this dataset
            continue
        ok, details, failed_cols = outcome
        results[name] = {"status": "PASS" if ok else "FAIL", "details": details, "failed_columns": failed_cols}
        if ok:
            passed += 1
        else:
            failed_n += 1
            # Task 4.3: every FAIL is logged at WARNING level.
            logger.warning("DQ FAIL [%s] %s -> %s", dataset_name, name, details if isinstance(details, str)
                           else {c: details[c] for c in failed_cols})
    return {
        "dataset_name": dataset_name,
        "checks_run": passed + failed_n,
        "checks_passed": passed,
        "checks_failed": failed_n,
        "results": results,
    }


# Critical = completeness below 80% on a non-optional column (used by the Airflow gate).
def critical_dq_failures(dq_result: dict, config: dict = None) -> list:
    """Critical = completeness below 80% on any non-optional column (used by the Airflow DQ gate)."""
    ds = dq_result["dataset_name"]
    optional = set((config or DQ_CONFIG).get(ds, {}).get("completeness_optional", []))
    details = dq_result["results"].get("completeness", {}).get("details", {})
    if not isinstance(details, dict):
        return []
    return [f"{ds}.{col} completeness {ratio:.0%} < {CRITICAL_COMPLETENESS:.0%}"
            for col, ratio in details.items() if ratio < CRITICAL_COMPLETENESS and col not in optional]


# The gate: raise ValueError when any critical failure exists, otherwise return the results.
def enforce_dq_gate(datasets: dict, config: dict = None) -> dict:
    """
    DQ gate used by the Airflow DAG (Task 3.3d): run the checks on every dataset and
    raise ValueError if any non-optional column has completeness below 80%.
    `datasets` maps dataset name -> DataFrame. Returns the DQ results (also on success).
    """
    results = {name: run_data_quality_checks(df, name, config=config, reference_tables=datasets)
               for name, df in datasets.items()}
    critical = [msg for res in results.values() for msg in critical_dq_failures(res, config)]
    if critical:
        raise ValueError("Data quality gate FAILED - critical issues: " + "; ".join(critical))
    return results


# Human-readable markdown report, one per dataset.
def write_dq_report(dq_result: dict, output_dir: str = None) -> str:
    """Write outputs/dq_report_<dataset>.md for humans."""
    output_dir = output_dir or OUTPUT_DIR
    ds = dq_result["dataset_name"]
    lines = [f"# Data Quality Report — {ds}", "",
             f"_Generated {datetime.now().isoformat(timespec='seconds')}_", "",
             f"**Checks run:** {dq_result['checks_run']} · **Passed:** {dq_result['checks_passed']} · "
             f"**Failed:** {dq_result['checks_failed']}", "",
             "| Check | Status | Failed columns | Details |", "|---|---|---|---|"]
    for name, r in dq_result["results"].items():
        det = r["details"]
        if isinstance(det, dict):
            det = ", ".join(f"{k}={v:.0%}" for k, v in det.items())
        lines.append(f"| {name} | {'✅ PASS' if r['status'] == 'PASS' else '❌ FAIL'} | "
                     f"{', '.join(r['failed_columns']) or '—'} | {str(det).replace('|', '/')} |")
    path = os.path.join(output_dir, f"dq_report_{ds}.md")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    return path


# ==============================================================================
# PIPELINE ENTRY POINT
# ==============================================================================



def main():
    """Standalone run: load + clean all datasets, run the checks, write markdown reports."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", help="optional YAML rules file (same structure as DQ_CONFIG)")
    ap.add_argument("--out", default=OUTPUT_DIR)
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)
    cfg = load_dq_config(args.config)

    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "02_sql_and_viz"))
    import etl_full as etl  # imported here (not at top) because etl_full itself imports this module
    projects = etl.transform_projects(etl.load_projects(os.path.join(etl.DATA_DIR, "projects.csv")))
    history = pd.read_csv(os.path.join(etl.DATA_DIR, "employees_salary_history.csv"))
    employees = etl.clean_employees(etl.load_employees(os.path.join(etl.DATA_DIR, "employees.csv")), history)
    transactions = etl.enrich_transactions(
        etl.load_transactions(os.path.join(etl.DATA_DIR, "transactions.json")), projects, employees)
    refs = {"projects": projects, "employees": employees}
    for name, df in [("projects", projects), ("employees", employees), ("transactions", transactions),
                     ("employees_salary_history", history)]:
        res = run_data_quality_checks(df, name, config=cfg, reference_tables=refs)
        print(f"{name}: {res['checks_passed']}/{res['checks_run']} checks passed ->",
              write_dq_report(res, args.out))


if __name__ == "__main__":
    main()
