"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
Solution File: etl_pipeline.py
Pillar: Foundations (Tasks 1.1, 1.3)
Author: Mohammed Faisal
=============================================================

Task 1.1  load_projects / transform_projects   -> outputs/.../01_foundations/projects_clean.csv
Task 1.3  load_employees / clean_employees     -> outputs/.../01_foundations/employees_clean.csv
                                                  + employees_quality_summary.json

Imported (not duplicated) by 02_sql_and_viz/etl_full.py, 04_infrastructure/dq_framework.py
and 03_big_data/airflow_dag.py.

HOW TO RUN
----------
  python solutions/submissions/mohammed_faisal/01_foundations/etl_pipeline.py

ENV (optional): DATA_DIR (default <repo>/datasets), OUTPUT_DIR (default outputs/results/mohammed_faisal/01_foundations)
"""

import json
import logging
import os
import time
from datetime import datetime

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# ── Paths ──────────────────────────────────────────────────────────────────────
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
RESULTS_DIR = os.path.join(REPO_ROOT, "outputs", "results", "mohammed_faisal")
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(REPO_ROOT, "datasets"))
# Detect the Airflow containers, where datasets/ and outputs/ are mounted under /opt/airflow.
IN_AIRFLOW = "DATA_DIR" not in os.environ and not os.path.isdir(DATA_DIR) and os.path.isdir("/opt/airflow/datasets")
if IN_AIRFLOW:      # docker-compose mounts datasets/ and outputs/ under /opt/airflow
    DATA_DIR = "/opt/airflow/datasets"


# Resolve the output folder: env var > Airflow mount > outputs/results/<me>/<pillar>.
def default_output_dir(pillar_folder: str) -> str:
    """OUTPUT_DIR env var, else /opt/airflow/outputs inside Airflow, else outputs/results/<me>/<pillar>."""
    if "OUTPUT_DIR" in os.environ:
        return os.environ["OUTPUT_DIR"]
    return "/opt/airflow/outputs" if IN_AIRFLOW else os.path.join(RESULTS_DIR, pillar_folder)


OUTPUT_DIR = default_output_dir("01_foundations")

# Decisions / counts collected during a run and reported by write_outputs()
# Counts and decisions collected during a run; write_outputs() turns them into the summary files.
RUN_STATS = {"row_counts": {}, "decisions": [], "quality_fixes": {}, "detection": {}}


# Remember raw/clean row counts per dataset for the summary.
def _record_count(name: str, raw=None, clean=None):
    entry = RUN_STATS["row_counts"].setdefault(name, {})
    if raw is not None:
        entry["raw"] = int(raw)
    if clean is not None:
        entry["clean"] = int(clean)


# Remember a data-quality decision (once) so it appears in pipeline_summary.txt.
def _decision(text: str):
    if text not in RUN_STATS["decisions"]:
        RUN_STATS["decisions"].append(text)


# ==============================================================================
# TASK 1.1 — Load and transform projects.csv
# ==============================================================================

def load_projects(filepath: str) -> pd.DataFrame:
    """
    Load projects.csv with explicit dtypes and add the derived metrics.

    Derived columns keep NULL when the inputs are unknown (we don't invent
    numbers); they are computed BEFORE nulls in budget/actual_cost are zero-filled
    in transform_projects().
    """
    logger.info("Loading projects data...")

    df = pd.read_csv(
        filepath,
        dtype={
            "project_id": "string", "project_name": "string", "department": "string",
            "status": "string", "project_manager_id": "string",
            "priority": "string", "region": "string",
        },
        parse_dates=["start_date", "end_date"],   # -> datetime64, NaT if missing
    )
    _record_count("projects", raw=len(df))

    # budget / actual_cost: floats so that nulls survive until transform step
    df["budget"] = pd.to_numeric(df["budget"], errors="coerce")
    df["actual_cost"] = pd.to_numeric(df["actual_cost"], errors="coerce")

    # budget_variance: positive = overspend; NaN when either side unknown
    df["budget_variance"] = df["actual_cost"] - df["budget"]

    # is_over_budget: False when either value is null (cannot prove overspend)
    df["is_over_budget"] = (df["actual_cost"] > df["budget"]).fillna(False).astype(bool)

    # duration_days: only where both dates exist (NaT -> <NA>)
    df["duration_days"] = (df["end_date"] - df["start_date"]).dt.days.astype("Int64")

    # budget_utilisation_pct: guard against div-by-zero / null budget -> NaN
    safe_budget = df["budget"].where(df["budget"] > 0)
    df["budget_utilisation_pct"] = (df["actual_cost"] / safe_budget * 100).round(2)

    logger.info("Loaded %d projects (%d columns)", len(df), df.shape[1])
    return df


def transform_projects(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise status, categorise, zero-fill money nulls, add risk_level."""
    logger.info("Transforming projects data...")
    df = df.copy()

    # Business mapping required by Task 1.1: raw status -> Active / Closed / Pending.
    status_map = {
        "In Progress": "Active",
        "Completed": "Closed",
        "Not Started": "Pending",
        "On Hold": "Pending"
    }

    # Standardise status: trim + collapse whitespace + title case
    df["status"] = (
        df["status"].astype("string").str.strip().str.replace(r"\s+", " ", regex=True).str.title()
    )
    df["status_category"] = df["status"].map(status_map)
    unmapped = df["status_category"].isna()
    if unmapped.any():
        logger.warning("%d projects have an unmapped status; category set to 'Unknown'", unmapped.sum())
        df.loc[unmapped, "status_category"] = "Unknown"

    # Null money values -> 0 (requirement). Derived columns were computed earlier
    # so unknowns stay NULL there rather than showing misleading numbers.
    null_budget, null_cost = df["budget"].isna().sum(), df["actual_cost"].isna().sum()
    df[["budget", "actual_cost"]] = df[["budget", "actual_cost"]].fillna(0)
    RUN_STATS["quality_fixes"]["projects: null budget -> 0"] = int(null_budget)
    RUN_STATS["quality_fixes"]["projects: null actual_cost -> 0"] = int(null_cost)
    _decision("projects: null budget/actual_cost replaced with 0; derived variance/utilisation "
              "stay NULL where an input was unknown, is_over_budget = False when unknown.")

    # risk_level — vectorised with np.select (first matching rule wins)
    df["risk_level"] = np.select(
        [   # plain numpy bool arrays: np.select rejects pandas nullable booleans on older pandas
            ((df["priority"] == "Critical") | df["is_over_budget"]).fillna(False).to_numpy(dtype=bool),
            ((df["priority"] == "High") | (df["budget_utilisation_pct"] > 90)).fillna(False).to_numpy(dtype=bool),
        ],
        ["High", "Medium"],
        default="Low",
    )

    _record_count("projects", clean=len(df))
    return df


# ==============================================================================
# TASK 1.3 — Data quality issues in employees.csv
# ==============================================================================

# Salary band (AED / month) that is plausible per level — business rule config.
# Plausible monthly salary range (AED) per level; used to detect logical inconsistencies.
LEVEL_SALARY_BANDS = {
    "Junior": (12_000, 20_000),
    "Mid": (17_000, 24_000),
    "Senior": (25_000, 36_000),
    "Lead": (38_000, 50_000),
    "Director": (50_000, 70_000),
}
# Plausible experience (years) per level.
# Plausible years of experience per level.
LEVEL_EXPERIENCE_BANDS = {
    "Junior": (0, 3), "Mid": (0, 6), "Senior": (0, 12), "Lead": (8, 18), "Director": (12, 25),
}
# Earliest hire date accepted as plausible.
HIRE_DATE_MIN = pd.Timestamp("1990-01-01")


# Loaded with hire_date as text so invalid values survive until clean_employees() reports them.
def load_employees(filepath: str) -> pd.DataFrame:
    """Load employees.csv as-is (strings for dates, so bad values are preserved) and log a profile."""
    logger.info("Loading employees data...")
    df = pd.read_csv(filepath, dtype={"hire_date": "string"})
    _record_count("employees", raw=len(df))
    logger.info("Employees: %d rows x %d columns", *df.shape)
    nulls = df.isna().sum()
    logger.info("Null counts per column: %s", nulls[nulls > 0].to_dict() or "none")
    return df


def clean_employees(df: pd.DataFrame, salary_history: pd.DataFrame = None) -> pd.DataFrame:
    """
    Detect (vectorised) and fix data-quality issues in employees.csv.

    Every fix is logged with its affected row count and is also recorded in the
    per-row `dq_flags` column so that changes stay auditable.
    salary_history (optional) is used as an evidence source for imputation.
    """
    logger.info("Cleaning employees data...")
    df = df.copy()
    # Per-row audit trail: every fix appends a label so changes stay traceable.
    flags = pd.Series("", index=df.index, dtype="string")
    fixes = {}  # issue -> rows affected (quality summary)

    def flag(mask, label):
        nonlocal flags
        flags = flags.where(~mask, flags + label + ";")

    # ---- Issue 1: missing / empty required text values -------------------------
    # Emails missing in 10 rows. Fix: derive from the company convention
    # first.last@presight.ai (multi-word surnames joined with '.'); on collision
    # append the employee number so emails stay unique.

    #I use Boolean masks to detect nulls and blank strings across required fields. I count and report missing values. For missing emails, I generate addresses using the employee’s name and add the employee ID if the address conflicts with another one. I also record the changes for auditing
    any_missing = pd.Series(False, index=df.index)
    for col in ["employee_id", "full_name", "department", "role", "level", "status"]:
        blank = df[col].isna() | (df[col].astype("string").str.strip() == "")
        any_missing |= blank
        if blank.any():
            logger.warning("Missing %s in %d rows (cannot be derived; left null)", col, blank.sum())
        fixes[f"missing {col} (unfixable)"] = int(blank.sum())

    missing_email = df["email"].isna() | (df["email"].astype("string").str.strip() == "")
    any_missing |= missing_email
    logger.info("[DQ] Missing emails: %d", missing_email.sum())
    derived = (
        df.loc[missing_email, "full_name"].str.lower().str.strip()
        .str.replace(r"[^a-z\s]", "", regex=True).str.replace(r"\s+", ".", regex=True)
    )
    derived = derived + "@presight.ai"
    taken = set(df.loc[~missing_email, "email"])
    collision = derived.isin(taken) | derived.duplicated(keep=False)
    with_id = derived.str.replace("@presight.ai", "", regex=False) + "." + df.loc[missing_email, "employee_id"].str.lower() + "@presight.ai"
    derived = derived.where(~collision, with_id)
    df.loc[missing_email, "email"] = derived
    flag(missing_email, "email_derived")
    fixes["missing email -> derived from name"] = int(missing_email.sum())

    # ---- Issue 2: invalid date formats -----------------------------------------
    # hire_date holds sentinels like '-999' and '99999-01-01' that do not parse.
    #I parse hire dates using the expected year-month-day format. Invalid values become missing dates so the pipeline can continue. I log the affected count and original values, then store the count in the quality report.
    parsed = pd.to_datetime(df["hire_date"], errors="coerce", format="%Y-%m-%d")
    invalid_date = parsed.isna()
    logger.info("[DQ] Unparseable hire_date: %d (values: %s)", invalid_date.sum(),
                df.loc[invalid_date, "hire_date"].value_counts().to_dict())
    df["hire_date"] = parsed
    fixes["invalid hire_date format -> NULL"] = int(invalid_date.sum())

    # ---- Issue 3: implausible dates --------------------------------------------
    # Parsable but outside business range (before 1990 or in the future).

    #After parsing hire dates, I validate them against a business range. Dates before 1990 or in the future are marked as missing. I log the count and flag affected employees so the issues remain traceable.
    implausible = parsed.notna() & ((parsed < HIRE_DATE_MIN) | (parsed > pd.Timestamp.today()))
    logger.info("[DQ] Implausible hire_date (<%s or future): %d", HIRE_DATE_MIN.date(), implausible.sum())
    df.loc[implausible, "hire_date"] = pd.NaT
    fixes["implausible hire_date -> NULL"] = int(implausible.sum())
    bad_date = invalid_date | implausible
    flag(bad_date, "hire_date_invalid")

    # Recover from the salary-history 'Hire' record when we have one.
    recovered = 0
    if salary_history is not None and bad_date.any():
        hire_rows = salary_history[salary_history["change_type"] == "Hire"]
        hire_map = pd.to_datetime(hire_rows.set_index("employee_id")["effective_date"], errors="coerce")
        hire_map = hire_map[~hire_map.index.duplicated()]
        cand = df.loc[bad_date, "employee_id"].map(hire_map)
        df.loc[bad_date, "hire_date"] = cand
        recovered = int(cand.notna().sum())
    logger.info("[FIX] hire_date recovered from salary history: %d of %d", recovered, bad_date.sum())
    fixes["hire_date recovered from salary history"] = recovered

    # ---- Issue 4: numeric out of range -----------------------------------------
    # years_experience < 0 is impossible -> replace with the median for the level.
    # Absolute plausibility ranges (independent of level): salary 10K-100K AED, experience 0-50 years.

    #I detect salary and experience values outside configured ranges. For negative experience, I replace the value with the median for the employee’s level. I use the median because it is less sensitive to extreme values, and I flag each replacement for auditing
    abs_off = ((df["salary"] < 10_000) | (df["salary"] > 100_000)
               | (df["years_experience"] < 0) | (df["years_experience"] > 50))
    logger.info("[DQ] Numeric values outside absolute range (salary 10K-100K, experience 0-50): %d", abs_off.sum())
    neg_exp = df["years_experience"] < 0
    logger.info("[DQ] Negative years_experience: %d", neg_exp.sum())
    level_median_exp = df["level"].map(df.loc[~neg_exp].groupby("level")["years_experience"].median())
    df.loc[neg_exp, "years_experience"] = level_median_exp[neg_exp].round().astype(int)
    flag(neg_exp, "experience_imputed")
    fixes["negative years_experience -> level median"] = int(neg_exp.sum())

    # Experience not fitting the level (e.g. Junior with 10 years)

    #I map each employee’s level to its expected experience range, then use vectorised comparisons to detect mismatches. I log and flag these records for review because being outside the expected range does not necessarily mean the value is wrong.
    exp_lo = df["level"].map({k: v[0] for k, v in LEVEL_EXPERIENCE_BANDS.items()})
    exp_hi = df["level"].map({k: v[1] for k, v in LEVEL_EXPERIENCE_BANDS.items()})
    exp_off = (df["years_experience"] < exp_lo) | (df["years_experience"] > exp_hi)
    logger.info("[DQ] years_experience outside level band (after fix): %d", exp_off.sum())
    flag(exp_off, "experience_level_mismatch")
    fixes["years_experience outside level band (flagged only)"] = int(exp_off.sum())

    # ---- Issue 5: logical inconsistency — salary vs level ------------------------
    # Juniors paid 66-76k are ~5x the level cap (look like x5 keying errors).
    # Fix: scale back if the /5 value lands in band, otherwise use level median.
    #I validate salaries against level-specific bands. For outliers, I first apply an assumed factor-of-five correction only if it brings the salary into range. Otherwise, I use the median of originally valid salaries at the same level. I flag the changes for auditing.
    lo = df["level"].map({k: v[0] for k, v in LEVEL_SALARY_BANDS.items()})
    hi = df["level"].map({k: v[1] for k, v in LEVEL_SALARY_BANDS.items()})
    salary_off = (df["salary"] < lo) | (df["salary"] > hi)
    logger.info("[DQ] Salary outside band for level: %d -> %s", salary_off.sum(),
                df.loc[salary_off, ["employee_id", "level", "salary"]].values.tolist())
    for factor in (5,):
        scaled = df["salary"] / factor
        in_band = salary_off & (scaled >= lo) & (scaled <= hi)
        df.loc[in_band, "salary"] = scaled[in_band].round().astype(int)
    still_off = salary_off & ((df["salary"] < lo) | (df["salary"] > hi))
    level_median_sal = df["level"].map(df.loc[~salary_off].groupby("level")["salary"].median())
    df.loc[still_off, "salary"] = level_median_sal[still_off].round().astype(int)
    flag(salary_off, "salary_corrected")
    fixes["salary outside level band -> corrected"] = int(salary_off.sum())

    # ---- Issue 6: status conflicts / referential issues --------------------------
    # (a) Active employees whose hire date is missing/invalid/future cannot be
    #     verified as genuinely employed -> flagged for HR review (status kept).
    still_bad_date = df["hire_date"].isna()
    conflict_active = still_bad_date & (df["status"] == "Active")
    logger.info("[DQ] Active employees with no valid hire_date after recovery: %d", conflict_active.sum())
    flag(conflict_active, "active_without_hire_date_REVIEW")
    fixes["Active without verifiable hire_date (flagged for review)"] = int(conflict_active.sum())

    # (b) Manager placeholder 'EMP0000' does not exist in the table -> NULL (top of hierarchy).
    orphan_mgr = df["manager_id"].notna() & ~df["manager_id"].isin(df["employee_id"])
    logger.info("[DQ] manager_id not present in employees: %d (%s)", orphan_mgr.sum(),
                df.loc[orphan_mgr, "manager_id"].unique().tolist())
    df.loc[orphan_mgr, "manager_id"] = pd.NA
    flag(orphan_mgr, "manager_placeholder_nulled")
    fixes["manager_id placeholder (no such employee) -> NULL"] = int(orphan_mgr.sum())

    # (c) Inactive employees still referenced as managers of active staff.
    inactive_ids = df.loc[df["status"] == "Inactive", "employee_id"]
    inactive_mgr = df["manager_id"].isin(inactive_ids) & (df["status"] == "Active")
    logger.info("[DQ] Active staff reporting to an Inactive manager: %d", inactive_mgr.sum())
    flag(inactive_mgr, "manager_inactive_REVIEW")
    fixes["Active staff with Inactive manager (flagged)"] = int(inactive_mgr.sum())

    # ---- Housekeeping: whitespace / duplicates ----------------------------------
    #I trim leading and trailing whitespace and count the changed values. I log duplicate IDs and emails, then keep the first record per employee ID. Finally, I attach the accumulated data-quality flags to the remaining rows so the cleaning actions are traceable
    for col in ["full_name", "department", "role", "level", "region", "status"]:
        stripped = df[col].astype("string").str.strip()
        fixes.setdefault("whitespace trimmed", 0)
        fixes["whitespace trimmed"] += int((stripped != df[col]).sum())
        df[col] = stripped
    dup_ids = df["employee_id"].duplicated().sum()
    dup_emails = df["email"].duplicated().sum()
    logger.info("[DQ] Duplicate employee_id: %d | duplicate email: %d", dup_ids, dup_emails)
    df = df.drop_duplicates(subset="employee_id", keep="first")
    fixes["duplicate employee_id dropped"] = int(dup_ids)

    df["dq_flags"] = flags.loc[df.index].str.rstrip(";")

    # ---- Detection report (one line per issue category, counts of affected rows) ----
    detected = {
        "Missing values": int(any_missing.sum()),
        "Invalid date formats": int(invalid_date.sum()),
        "Implausible dates": int(implausible.sum()),
        "Numeric out-of-range": int(abs_off.sum()),
        "Logical inconsistencies (salary/experience vs level)": int((salary_off | exp_off).sum()),
        "Status conflicts": int((conflict_active | inactive_mgr).sum()),
        "Referential issues (manager_id)": int(orphan_mgr.sum()),
        "Duplicates": int(dup_ids),
    }
    logger.info("=== EMPLOYEES DETECTION REPORT ===")
    for k, v in detected.items():
        logger.info("  %-55s %5d rows", k, v)
    RUN_STATS["detection"] = detected

    # ---- Quality summary --------------------------------------------------------
    logger.info("Employee quality summary (rows affected per fix type):")
    for k, v in fixes.items():
        logger.info("  %-58s %5d", k, v)
    RUN_STATS["quality_fixes"].update({f"employees: {k}": v for k, v in fixes.items()})
    _record_count("employees", clean=len(df))
    _decision("employees: bad hire_dates set to NULL then recovered from salary history where possible; "
              "salary/experience corrected using level rules; issues flagged in dq_flags column.")
    return df


# ==============================================================================
# OUTPUTS — used by this file's main and by 02_sql_and_viz/etl_full.py
# ==============================================================================

def write_outputs(frames: dict, out_dir: str = None, elapsed_seconds: float = None) -> dict:
    """
    Write each DataFrame in `frames` ({"projects_clean": df, ...}) as <name>.csv plus
    pipeline_summary.txt; when employees_clean is present also employees_quality_summary.json
    (detection counts + rows affected per fix type). Returns {name: path}.
    """
    # Default to this module's output folder unless the caller passes one.
    out_dir = out_dir or OUTPUT_DIR
    os.makedirs(out_dir, exist_ok=True)
    written = {}
    # One CSV per DataFrame; dates are written as YYYY-MM-DD.
    for name, frame in frames.items():
        written[name] = os.path.join(out_dir, f"{name}.csv")
        frame.to_csv(written[name], index=False, date_format="%Y-%m-%d")
        logger.info("Wrote %s (%d rows x %d columns)", written[name], *frame.shape)

    # Task 1.3 deliverable: machine-readable quality summary (detection counts + rows affected per fix).
    if "employees_clean" in frames:
        emp = RUN_STATS["row_counts"].get("employees", {})
        fixes = {k.split(": ", 1)[1]: v for k, v in RUN_STATS["quality_fixes"].items() if k.startswith("employees: ")}
        # Structure of employees_quality_summary.json.
        summary = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "rows_before": emp.get("raw"), "rows_after": emp.get("clean"),
            "issues_detected_rows": RUN_STATS["detection"],
            "rows_affected_per_fix_type": fixes,
            "rows_with_at_least_one_flag": int((frames["employees_clean"]["dq_flags"].fillna("") != "").sum()),
        }
        written["employees_quality_summary"] = os.path.join(out_dir, "employees_quality_summary.json")
        with open(written["employees_quality_summary"], "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=2)

    # Build the human-readable pipeline_summary.txt (counts, fixes, decisions, files written).
    lines = ["PRESIGHT PIPELINE SUMMARY", "=" * 60,
             f"Run timestamp     : {datetime.now().isoformat(timespec='seconds')}"]
    if elapsed_seconds is not None:
        lines.append(f"Pipeline duration : {elapsed_seconds:.2f} s")
    lines += ["", "ROW COUNTS (raw -> clean)", "-" * 60]
    lines += [f"  {n:<14} raw={c.get('raw', 'n/a'):>7}   clean={c.get('clean', 'n/a'):>7}"
              for n, c in RUN_STATS["row_counts"].items()]
    if RUN_STATS["detection"]:
        lines += ["", "EMPLOYEES DETECTION REPORT (rows affected per issue)", "-" * 60]
        lines += [f"  {k:<58} {v:>5}" for k, v in RUN_STATS["detection"].items()]
    lines += ["", "DATA QUALITY FIXES (rows affected)", "-" * 60]
    lines += [f"  {k:<64} {v:>6}" for k, v in RUN_STATS["quality_fixes"].items()]
    lines += ["", "DECISIONS", "-" * 60] + [f"  - {d}" for d in RUN_STATS["decisions"]]
    lines += ["", "FILES WRITTEN", "-" * 60] + [f"  {p}" for p in written.values()]
    written["pipeline_summary"] = os.path.join(out_dir, "pipeline_summary.txt")
    with open(written["pipeline_summary"], "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    return written


def run_foundations(out_dir: str = None) -> dict:
    """Pillar 1: Task 1.1 (projects) + Task 1.3 (employees)."""
    t0 = time.time()
    projects = transform_projects(load_projects(os.path.join(DATA_DIR, "projects.csv")))
    history_path = os.path.join(DATA_DIR, "employees_salary_history.csv")
    # The salary history is optional; it only helps to recover missing hire dates.
    history = pd.read_csv(history_path) if os.path.exists(history_path) else None
    employees = clean_employees(load_employees(os.path.join(DATA_DIR, "employees.csv")), history)
    # Write projects_clean.csv, employees_clean.csv, the quality summary and pipeline_summary.txt.
    written = write_outputs({"projects_clean": projects, "employees_clean": employees},
                            out_dir, time.time() - t0)
    logger.info("Foundations complete in %.2fs", time.time() - t0)
    return written


if __name__ == "__main__":
    run_foundations()
