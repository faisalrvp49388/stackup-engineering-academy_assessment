-- =============================================================================
-- StackUp Engineering Academy — Data Engineering Assessment
-- Solution File: data_model.sql
-- Pillar: Foundations (Task 1.2) — star schema DDL, SCD2 dim_employee build,
--         and the staging load that Pillar 2's queries run against
-- Author: Mohammed Faisal | Engine: DuckDB (also valid PostgreSQL for the DDL
--         / queries, except read_csv_auto in the staging loads)
-- =============================================================================
--
-- SCENARIO
-- --------
-- Presight runs a project management platform. This file designs and loads
-- the analytics warehouse that powers dashboards for Finance, Operations,
-- and Executive leadership. The business questions and query-optimisation
-- exercise that query this warehouse live in the sibling 02_sql_and_viz/
-- files: queries.sql (Task 2.1) and query_optimization.sql (Task 2.3).
--
-- PREREQUISITES
-- -------------
-- Run everything from the repo root and have Pillar 1/2's cleaning outputs
-- on disk first:
--   python solutions/submissions/mohammed_faisal/01_foundations/etl_pipeline.py
--     -> outputs/results/mohammed_faisal/01_foundations/{projects,employees}_clean.csv
--   python solutions/submissions/mohammed_faisal/02_sql_and_viz/etl_full.py
--     -> outputs/results/mohammed_faisal/02_sql_and_viz/transactions_clean.csv
--        (Task 2.2's cleaned/enriched transactions — fact_transactions and
--        dim_vendor are built from this file, not from the raw
--        datasets/transactions.json, so Task 2.2 must run before this file)
--
-- HOW TO USE
-- ----------
-- Both sections use repo-relative paths (no path placeholders to substitute),
-- so this file can be run directly in any SQL client (DuckDB CLI, SQLTools
-- against outputs/presight_warehouse.duckdb, etc.) as long as the client's
-- working directory is the repo root. The Reset block at the top of Section 1
-- makes the whole file safely re-runnable against an existing warehouse.
-- The full driver that also runs queries.sql and query_optimization.sql
-- afterwards is:
--   python solutions/submissions/mohammed_faisal/02_sql_and_viz/run_sql.py
--
-- FILE LAYOUT
-- -----------
--   SECTION 1 — Task 1.2: star schema DDL (6 tables), SCD2 dim_employee
--               build, and the SCD2 validation queries (Q1-Q6)
--   SECTION 2 — Load the remaining five tables from the staging data, then
--               reconcile row counts across the whole star schema
-- =============================================================================


-- ===========================================================================
-- SECTION 1 — TASK 1.2: Design the data model (Star Schema + SCD Type 2)
-- ===========================================================================
--
-- Kimball star schema: one fact table (fact_transactions) at the centre,
-- surrounded by conformed dimensions and one bridge table for the
-- employee <-> project many-to-many. Every table uses a surrogate INTEGER
-- key (*_key) as its primary key and keeps the source-system identifier
-- (project_id, employee_id, vendor_id, transaction_id) as a natural key:
--   1. dim_employee is SCD2 — employee_id repeats once per version, so only
--      a surrogate can uniquely identify "this person as they were then".
--   2. Integer keys are cheaper to join and index than VARCHAR natural keys.
--   3. Surrogates are assigned by ROW_NUMBER() over the natural key, so they
--      are deterministic and reproducible across rebuilds.

-- ===========================================================================
-- Reset — makes this whole file safely re-runnable
-- ===========================================================================
-- DROP (rather than DELETE) so a changed column list in the DDL below is
-- always picked up on the next run. Dropped in reverse FK order —
-- fact_transactions and bridge_employee_project reference the dim_* tables,
-- so they go first; dropping a parent a child still references would fail.
DROP TABLE IF EXISTS fact_transactions;
DROP TABLE IF EXISTS bridge_employee_project;
DROP TABLE IF EXISTS dim_vendor;
DROP TABLE IF EXISTS dim_employee;
DROP TABLE IF EXISTS dim_project;
DROP TABLE IF EXISTS dim_date;

-- ---------------------------------------------------------------------------
-- dim_date
-- ---------------------------------------------------------------------------
-- Design decision: a real calendar dimension, generated (not derived from
-- the facts) so every day exists even when there is no spend, and so BI
-- tools can slice by any calendar attribute without date functions in every
-- query. date_key is a smart integer YYYYMMDD: human readable, sortable and
-- partition friendly. year_month ('YYYY-MM') is pre-computed for trend
-- charts. The UAE work week has been Mon-Fri since 2022, so Sat/Sun are
-- the weekend.
CREATE TABLE dim_date (
    date_key      INTEGER PRIMARY KEY,          -- YYYYMMDD
    full_date     DATE     NOT NULL UNIQUE,     -- natural key
    year          SMALLINT NOT NULL,
    quarter       SMALLINT NOT NULL,
    month         SMALLINT NOT NULL,
    month_name    VARCHAR  NOT NULL,
    year_month    VARCHAR  NOT NULL,            -- 'YYYY-MM' (handy for trend charts)
    week          SMALLINT NOT NULL,            -- ISO week number
    day           SMALLINT NOT NULL,
    day_of_week   SMALLINT NOT NULL,            -- 1=Mon ... 7=Sun (ISO)
    day_name      VARCHAR  NOT NULL,
    is_weekend    BOOLEAN  NOT NULL
);

-- ---------------------------------------------------------------------------
-- dim_project
-- ---------------------------------------------------------------------------
-- Design decision: Type 1 dimension (overwrite on change) — project
-- attributes are descriptive and analysts want the latest status, not a
-- history of it, so there is no valid_from/valid_to here. The Task 1.1
-- derived columns (status_category, budget_variance, is_over_budget,
-- budget_utilisation_pct, risk_level) are stored on the dimension rather
-- than recomputed per query, so every dashboard shares one definition.
-- Money columns are DECIMAL, not DOUBLE, so sums reconcile to the fils.
-- project_manager_id is kept as a natural-key attribute, not an FK to
-- dim_employee: that table's key identifies a *version*, not a person, and
-- the manager relationship is modelled properly in bridge_employee_project.
CREATE TABLE dim_project (
    project_key            INTEGER PRIMARY KEY,
    project_id             VARCHAR NOT NULL UNIQUE,     -- natural key
    project_name           VARCHAR NOT NULL,
    department             VARCHAR NOT NULL,
    status                 VARCHAR NOT NULL,
    status_category        VARCHAR NOT NULL,            -- Active / Closed / Pending
    start_date             DATE,
    end_date               DATE,
    budget                 DECIMAL(18,2) NOT NULL,
    actual_cost            DECIMAL(18,2) NOT NULL,
    budget_variance        DECIMAL(18,2),
    is_over_budget         BOOLEAN NOT NULL,
    budget_utilisation_pct DECIMAL(9,2),
    risk_level             VARCHAR NOT NULL,
    priority               VARCHAR NOT NULL,
    region                 VARCHAR NOT NULL,
    project_manager_id     VARCHAR NOT NULL             -- natural key of manager
);

-- ---------------------------------------------------------------------------
-- dim_employee — SCD Type 2
-- ---------------------------------------------------------------------------
-- Design decision: one row per employee *version*. employees_salary_history
-- .csv shows salary/role/level changing over time (2-5 versions for the
-- employees who have history), and fact_transactions needs point-in-time
-- correctness — "who was this approver when they approved it", not just
-- who they are today. A Type 1 table would overwrite that on every reload.
--   employee_key : surrogate, unique per version (facts point at the
--                  version that was valid when the event happened)
--   employee_id  : natural key, repeats across versions
--   valid_from / valid_to : HALF-OPEN interval [valid_from, valid_to).
--                  valid_to of a version == valid_from of the next one, so
--                  periods touch but never overlap, and a point-in-time
--                  lookup is simply  d >= valid_from AND d < valid_to.
--   valid_to = 9999-12-31 is the sentinel for the current version.
--   is_current   : exactly one TRUE per employee_id (Q1 / Q2 below).
--   change_type / change_reason : carried from the history file (BONUS).
-- Descriptive attributes that are not tracked (name, email, department,
-- region ...) come from the cleaned employee master and are the same on
-- every version (Type 1 behaviour inside a Type 2 table). The UNIQUE and
-- CHECK constraints make the engine itself reject duplicate or inverted
-- versions, on top of the validation queries. Population logic is below,
-- after all six DDL statements.
CREATE TABLE dim_employee (
    employee_key     INTEGER PRIMARY KEY,               -- surrogate, per version
    employee_id      VARCHAR NOT NULL,                  -- natural key
    full_name        VARCHAR NOT NULL,
    email            VARCHAR,
    department       VARCHAR NOT NULL,
    role             VARCHAR NOT NULL,                  -- tracked (SCD2)
    level            VARCHAR NOT NULL,                  -- tracked (SCD2)
    salary           DECIMAL(12,2) NOT NULL,            -- tracked (SCD2)
    hire_date        DATE,
    manager_id       VARCHAR,
    region           VARCHAR,
    status           VARCHAR,
    years_experience INTEGER,
    valid_from       DATE    NOT NULL,
    valid_to         DATE    NOT NULL,                  -- 9999-12-31 = current
    is_current       BOOLEAN NOT NULL,
    change_type      VARCHAR,
    change_reason    VARCHAR,
    UNIQUE (employee_id, valid_from),
    CHECK (valid_from < valid_to)
);

-- ---------------------------------------------------------------------------
-- dim_vendor
-- ---------------------------------------------------------------------------
-- Design decision: vendors only exist as vendor_id + vendor_name on
-- transaction rows, so the dimension is derived from them (25 vendors, a
-- strict 1:1 id <-> name mapping verified during profiling). Kept as its
-- own dimension, rather than two text columns on the fact, so vendor
-- attributes (risk rating, country ...) can be added later without
-- touching the fact table.
CREATE TABLE dim_vendor (
    vendor_key  INTEGER PRIMARY KEY,
    vendor_id   VARCHAR NOT NULL UNIQUE,                -- natural key
    vendor_name VARCHAR NOT NULL
);

-- ---------------------------------------------------------------------------
-- bridge_employee_project
-- ---------------------------------------------------------------------------
-- Design decision: people and projects are many-to-many, which a star
-- schema can't express as a plain FK on either dimension. Two relationship
-- types come out of the source data: (a) the project manager on each
-- project and (b) every employee who approved transactions on a project.
-- role_in_project distinguishes them, and is part of the composite PK
-- because one person can hold both roles on the same project.
-- assigned_from / assigned_to record the span of the relationship, and
-- employee_key is the SCD2 version in effect at assigned_from.
CREATE TABLE bridge_employee_project (
    employee_key    INTEGER NOT NULL REFERENCES dim_employee (employee_key),
    project_key     INTEGER NOT NULL REFERENCES dim_project (project_key),
    role_in_project VARCHAR NOT NULL,                   -- 'Project Manager' | 'Approver'
    assigned_from   DATE,
    assigned_to     DATE,
    PRIMARY KEY (employee_key, project_key, role_in_project)
);

-- ---------------------------------------------------------------------------
-- fact_transactions
-- ---------------------------------------------------------------------------
-- Design decision: grain = one row per financial transaction — the most
-- granular event available, so nothing downstream needs a coarser
-- pre-aggregation. transaction_id is kept as a degenerate dimension; the
-- measure is amount (AED). employee_key is the *approver* and points at the
-- SCD2 version valid on the transaction date (see the point-in-time join in
-- Section 2); it is NULL for the ~5% of unapproved transactions
-- (is_approved = FALSE) rather than pointing at a fake "unknown" member.
-- Null source amounts are stored as 0 with amount_is_missing = TRUE so
-- totals stay usable while the data gap stays visible. category /
-- payment_status / currency / invoice_ref stay on the fact as degenerate
-- attributes — too low-cardinality and metadata-free to earn their own join.
CREATE TABLE fact_transactions (
    transaction_key   INTEGER PRIMARY KEY,
    transaction_id    VARCHAR NOT NULL UNIQUE,          -- degenerate dimension
    project_key       INTEGER NOT NULL REFERENCES dim_project (project_key),
    employee_key      INTEGER REFERENCES dim_employee (employee_key),  -- the approver
    vendor_key        INTEGER NOT NULL REFERENCES dim_vendor (vendor_key),
    date_key          INTEGER NOT NULL REFERENCES dim_date (date_key),
    amount            DECIMAL(18,2) NOT NULL,
    amount_is_missing BOOLEAN NOT NULL,
    category          VARCHAR NOT NULL,
    payment_status    VARCHAR NOT NULL,
    currency          VARCHAR NOT NULL,
    invoice_ref       VARCHAR,
    is_approved       BOOLEAN NOT NULL
);

-- ===========================================================================
-- Populate dim_employee (SCD Type 2)
-- ===========================================================================
-- Source: outputs/results/mohammed_faisal/01_foundations/employees_clean.csv
-- (Task 1.3 output — NOT the raw datasets/employees.csv) +
-- datasets/employees_salary_history.csv.
--
-- DESIGN DECISIONS:
--
-- 1. Only role / level / salary are versioned (Type 2). Everything else
--    (name, email, department, region ...) is carried from employees_clean
--    .csv onto every version — the history file has nothing else to
--    version against.
--
-- 2. 594 of 1,000 employees have salary history (1,826 rows, 2-5 versions
--    each). Every history row becomes one version; its valid_to is the NEXT
--    row's effective_date via LEAD() (half-open, so no "- 1 day" arithmetic
--    and no gaps or overlaps by construction).
--
-- 3. The CURRENT version's role/level/salary come from employees_clean.csv,
--    not from the last history row, so the current row always agrees with
--    the cleaned master (3 salaries were repaired in Task 1.3). Its
--    valid_from is still the last history effective_date, and it keeps
--    that row's change_type / change_reason.
--
-- 4. The remaining 406 employees have no history: one row with valid_from
--    = hire_date, valid_to = 9999-12-31, is_current = TRUE, and change_type
--    'Initial'. 8 of them also have no usable hire_date (nulled by Task 1.3
--    as unrecoverable) and get a 1900-01-01 sentinel instead of NULL, so
--    the NOT NULL / CHECK constraints and point-in-time joins still hold.
--
-- 5. Same-day duplicates: EMP0084 has two history rows dated 2025-03-02
--    (Annual Raise, then Promotion). Two versions with the same valid_from
--    would give the earlier one an empty [d, d) interval and violate
--    UNIQUE (employee_id, valid_from). Fix: keep only the LAST row in file
--    order per (employee_id, effective_date) — the terminal state of that
--    day (16,314 -> 23,354, Junior -> Mid).
--
-- 6. The build is split into staging tables, three tmp_* version sets and
--    one final INSERT that joins to stg_employees ONCE for the carried-
--    forward columns and assigns the surrogate key. The tmp_* tables are
--    dropped afterwards; the stg_* tables are kept for auditing.
-- ===========================================================================
CREATE OR REPLACE TABLE stg_employees AS
SELECT *
FROM read_csv_auto('outputs/results/mohammed_faisal/01_foundations/employees_clean.csv');

CREATE OR REPLACE TABLE stg_salary_hist AS
SELECT *
FROM read_csv_auto('datasets/employees_salary_history.csv');

-- ---------------------------------------------------------------------------
-- Same-day dedup (design decision 5): number rows in file order, then keep
-- the last row per (employee_id, effective_date). Employees without
-- same-day ties are untouched — 1,826 rows in, 1,825 out.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE stg_salary_hist_dedup AS
SELECT * EXCLUDE (file_row, rn)
FROM (
    SELECT h.*,
           row_number() OVER (PARTITION BY employee_id, effective_date
                              ORDER BY file_row DESC) AS rn
    FROM (SELECT *, row_number() OVER () AS file_row FROM stg_salary_hist) h
)
WHERE rn = 1;

-- ---------------------------------------------------------------------------
-- Closed (historical) versions: every deduplicated history row except each
-- employee's most recent. valid_to is chained from the NEXT row's
-- effective_date via LEAD(), so consecutive versions touch exactly.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE tmp_dim_employee_hist AS
WITH ranked AS (
    SELECT h.*,
           LEAD(CAST(effective_date AS DATE)) OVER (PARTITION BY employee_id
                                                    ORDER BY effective_date) AS next_effective_date
    FROM stg_salary_hist_dedup h
)
SELECT employee_id,
       new_role                     AS role,
       new_level                    AS level,
       new_salary                   AS salary,
       CAST(effective_date AS DATE) AS valid_from,
       next_effective_date          AS valid_to,
       FALSE                        AS is_current,
       change_type,
       change_reason
FROM ranked
WHERE next_effective_date IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Current version for employees WITH history — role/level/salary from
-- employees_clean.csv, validity and change metadata from the last history
-- row (design decision 3).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE tmp_dim_employee_current_with_hist AS
SELECT e.employee_id,
       e.role,
       e.level,
       e.salary,
       CAST(lh.effective_date AS DATE) AS valid_from,
       DATE '9999-12-31'               AS valid_to,
       TRUE                            AS is_current,
       lh.change_type,
       lh.change_reason
FROM stg_employees e
JOIN (
    SELECT *
    FROM stg_salary_hist_dedup
    QUALIFY row_number() OVER (PARTITION BY employee_id ORDER BY effective_date DESC) = 1
) lh ON lh.employee_id = e.employee_id;

-- ---------------------------------------------------------------------------
-- Single current version for employees WITHOUT history — 1900-01-01
-- sentinel for the 8 employees whose hire_date is also unknown (design
-- decision 4).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE tmp_dim_employee_no_hist AS
SELECT e.employee_id,
       e.role,
       e.level,
       e.salary,
       COALESCE(CAST(e.hire_date AS DATE), DATE '1900-01-01') AS valid_from,
       DATE '9999-12-31'                                      AS valid_to,
       TRUE                                                   AS is_current,
       'Initial'                                              AS change_type,
       'No salary history — single version from employee master' AS change_reason
FROM stg_employees e
WHERE e.employee_id NOT IN (SELECT employee_id FROM stg_salary_hist_dedup);

-- ---------------------------------------------------------------------------
-- Final assembly: union the three version sets, join once to stg_employees
-- for the carried-forward columns, assign the surrogate key in
-- (employee_id, valid_from) order.
-- ---------------------------------------------------------------------------
INSERT INTO dim_employee
SELECT row_number() OVER (ORDER BY t.employee_id, t.valid_from) AS employee_key,
       t.employee_id,
       e.full_name,
       e.email,
       e.department,
       t.role,
       t.level,
       t.salary,
       CAST(e.hire_date AS DATE) AS hire_date,
       e.manager_id,
       e.region,
       e.status,
       e.years_experience,
       t.valid_from,
       t.valid_to,
       t.is_current,
       t.change_type,
       t.change_reason
FROM (
    SELECT * FROM tmp_dim_employee_hist
    UNION ALL
    SELECT * FROM tmp_dim_employee_current_with_hist
    UNION ALL
    SELECT * FROM tmp_dim_employee_no_hist
) t
JOIN stg_employees e ON e.employee_id = t.employee_id;

DROP TABLE tmp_dim_employee_hist;
DROP TABLE tmp_dim_employee_current_with_hist;
DROP TABLE tmp_dim_employee_no_hist;
-- stg_employees / stg_salary_hist / stg_salary_hist_dedup are deliberately
-- NOT dropped — they're small and Q6 below reconciles against them.

-- ===========================================================================
-- SCD2 validation queries (Task 1.2 requirement)
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- Q1: No employee has more than one current record. Must return zero rows.
-- ---------------------------------------------------------------------------
SELECT employee_id, COUNT(*) AS current_count
FROM dim_employee
WHERE is_current = TRUE
GROUP BY employee_id
HAVING COUNT(*) > 1;

-- ---------------------------------------------------------------------------
-- Q2: The other half of Q1 — every employee has at least one current
-- record. Together, Q1 + Q2 prove exactly one current row per employee.
-- Must return zero rows.
-- ---------------------------------------------------------------------------
SELECT e.employee_id
FROM (SELECT DISTINCT employee_id FROM dim_employee) e
LEFT JOIN dim_employee c ON c.employee_id = e.employee_id AND c.is_current
WHERE c.employee_key IS NULL;

-- ---------------------------------------------------------------------------
-- Q3: Employees with the most version history. Expect 2-5 versions for the
-- 594 employees who appear in employees_salary_history.csv.
-- ---------------------------------------------------------------------------
SELECT employee_id, COUNT(*) AS version_count
FROM dim_employee
GROUP BY employee_id
ORDER BY version_count DESC, employee_id
LIMIT 10;

-- ---------------------------------------------------------------------------
-- Q4: Self-join to detect overlapping periods for the same employee. Two
-- half-open periods [from, to) overlap iff each starts before the other
-- ends. Must return zero rows.
-- ---------------------------------------------------------------------------
SELECT a.employee_id,
       a.employee_key AS key_a, b.employee_key AS key_b,
       a.valid_from AS a_from, a.valid_to AS a_to,
       b.valid_from AS b_from, b.valid_to AS b_to
FROM dim_employee a
JOIN dim_employee b
  ON a.employee_id = b.employee_id
 AND a.employee_key < b.employee_key
 AND a.valid_from < b.valid_to
 AND b.valid_from < a.valid_to;

-- ---------------------------------------------------------------------------
-- Q5: Gap check — the complement of Q4. Q4 proves versions never overlap;
-- this proves they never leave a gap either: every non-current version
-- must end exactly where the next one begins (half-open, so equality, no
-- +1 day). Must return zero rows.
-- ---------------------------------------------------------------------------
SELECT a.employee_id, a.valid_to AS ends, MIN(b.valid_from) AS next_starts
FROM dim_employee a
LEFT JOIN dim_employee b ON b.employee_id = a.employee_id AND b.valid_from > a.valid_from
WHERE NOT a.is_current
GROUP BY a.employee_id, a.valid_to, a.employee_key
HAVING a.valid_to <> MIN(b.valid_from) OR MIN(b.valid_from) IS NULL;

-- ---------------------------------------------------------------------------
-- Q6: Row-count reconciliation — dim_employee's row count must equal
-- (one row per deduplicated history record) + (one row per employee with
-- no history), computed independently of the build above. Catches dropped
-- or duplicated employees, which Q1-Q5 would not. Expected 1,825 + 406 =
-- 2,231; expected_rows and actual_rows must match.
-- ---------------------------------------------------------------------------
SELECT (SELECT COUNT(*) FROM dim_employee) AS actual_rows,
       (SELECT COUNT(*) FROM stg_salary_hist_dedup)
     + (SELECT COUNT(*) FROM stg_employees
        WHERE employee_id NOT IN (SELECT employee_id FROM stg_salary_hist)) AS expected_rows;


-- ===========================================================================
-- SECTION 2 — Load staging data into the star schema
-- ===========================================================================
-- Loads the cleaned Pillar 1/2 outputs into the remaining five tables.
-- dim_employee is already populated (Section 1, above) before this runs.
-- Paths are relative to the repo root (run_sql.py chdir's there).

-- ---------------------------------------------------------------------------
-- Staging
-- ---------------------------------------------------------------------------
-- Design decision: transactions are read straight from Task 2.2's output
-- (02_sql_and_viz/etl_full.py's enrich_transactions), which already
-- computed amount_aed (null amount -> 0.0) and is_approved — so that
-- business logic lives in one place. fact_transactions and dim_vendor
-- therefore depend on the Task 2.2 pipeline having run first, the same
-- dependency pattern dim_project / dim_employee have on Task 1.1/1.3.
-- Explicit CASTs pin the types rather than trusting CSV auto-detection.
CREATE OR REPLACE TABLE stg_projects AS
SELECT *
FROM read_csv_auto('outputs/results/mohammed_faisal/01_foundations/projects_clean.csv');

CREATE OR REPLACE TABLE stg_transactions AS
SELECT transaction_id, project_id, vendor_id, vendor_name, category,
       CAST(amount AS DOUBLE)         AS amount,
       CAST(amount_aed AS DOUBLE)     AS amount_aed,
       currency,
       CAST(transaction_date AS DATE) AS transaction_date,
       approved_by, payment_status, invoice_ref, notes,
       CAST(is_approved AS BOOLEAN)   AS is_approved
FROM read_csv_auto('outputs/results/mohammed_faisal/02_sql_and_viz/transactions_clean.csv');

-- ---------------------------------------------------------------------------
-- dim_date
-- ---------------------------------------------------------------------------
-- Design decision: generated with generate_series rather than loaded from a
-- file — every column is a pure function of the date, so there is no source
-- data, only a range to materialise. 2000-01-01 .. 2030-12-31 covers every
-- hire date, project date and transaction date with headroom either side.
INSERT INTO dim_date
SELECT CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key,
       d                                      AS full_date,
       year(d)                                AS year,
       quarter(d)                             AS quarter,
       month(d)                               AS month,
       monthname(d)                           AS month_name,
       strftime(d, '%Y-%m')                   AS year_month,
       weekofyear(d)                          AS week,
       day(d)                                 AS day,
       isodow(d)                              AS day_of_week,
       dayname(d)                             AS day_name,
       isodow(d) IN (6, 7)                    AS is_weekend
FROM (SELECT CAST(unnest(generate_series(DATE '2000-01-01', DATE '2030-12-31', INTERVAL 1 DAY)) AS DATE) AS d);

-- ---------------------------------------------------------------------------
-- dim_project
-- ---------------------------------------------------------------------------
-- Design decision: a straight 1:1 load from Pillar 1's projects_clean.csv —
-- Task 1.1's cleaning already produced exactly the columns this dimension
-- needs. project_key is assigned by ROW_NUMBER() on the natural key, so the
-- surrogate is stable across re-runs.
INSERT INTO dim_project
SELECT row_number() OVER (ORDER BY project_id) AS project_key,
       project_id,
       project_name,
       department,
       status,
       status_category,
       start_date,
       end_date,
       budget,
       actual_cost,
       budget_variance,
       is_over_budget,
       budget_utilisation_pct,
       risk_level,
       priority,
       region,
       project_manager_id
FROM stg_projects;

-- ---------------------------------------------------------------------------
-- dim_vendor
-- ---------------------------------------------------------------------------
-- Design decision: no vendor master exists, so the dimension is built by
-- de-duplicating vendor_id / vendor_name out of the transactions. Profiling
-- confirmed every vendor_id maps to exactly one vendor_name, so a plain
-- DISTINCT is sufficient (no near-duplicate names to reconcile).
INSERT INTO dim_vendor
SELECT row_number() OVER (ORDER BY vendor_id) AS vendor_key,
       vendor_id,
       vendor_name
FROM (SELECT DISTINCT vendor_id, vendor_name FROM stg_transactions);

-- ---------------------------------------------------------------------------
-- bridge_employee_project
-- ---------------------------------------------------------------------------
-- Design decision: two link types are unioned — project managers (span =
-- project start_date .. end_date) and approvers (span = first .. last
-- transaction they approved on that project). Each link resolves to the
-- SCD2 version valid at assigned_from; if none is valid then (e.g. the
-- project started before the employee's first recorded version, or
-- assigned_from is NULL) it falls back to the current version, so no
-- relationship is silently dropped.
INSERT INTO bridge_employee_project
WITH links AS (
    SELECT project_manager_id AS employee_id,
           project_id,
           'Project Manager'  AS role_in_project,
           start_date         AS assigned_from,
           end_date           AS assigned_to
    FROM stg_projects
    UNION ALL
    SELECT approved_by, project_id, 'Approver', MIN(transaction_date), MAX(transaction_date)
    FROM stg_transactions
    WHERE approved_by IS NOT NULL
    GROUP BY approved_by, project_id
)
SELECT COALESCE(ev.employee_key, cur.employee_key) AS employee_key,
       p.project_key,
       l.role_in_project,
       l.assigned_from,
       l.assigned_to
FROM links l
JOIN dim_project  p   ON p.project_id = l.project_id
JOIN dim_employee cur ON cur.employee_id = l.employee_id AND cur.is_current
LEFT JOIN dim_employee ev ON ev.employee_id = l.employee_id
                         AND l.assigned_from >= ev.valid_from AND l.assigned_from < ev.valid_to;

-- ---------------------------------------------------------------------------
-- fact_transactions
-- ---------------------------------------------------------------------------
-- Design decision: employee_key resolves via a point-in-time join
-- (transaction_date in [valid_from, valid_to)) instead of a plain join to
-- the current row — a 2022 transaction must resolve to whichever
-- dim_employee version was valid in 2022, not to today's row. This is the
-- design choice that makes dim_employee's SCD2 shape actually pay off.
-- Where no version covers the transaction date (25 transactions whose
-- approver's first recorded version starts after it), the join falls back to the approver's
-- current version rather than losing the approver; unapproved transactions
-- (approved_by NULL) keep a NULL employee_key.
INSERT INTO fact_transactions
SELECT row_number() OVER (ORDER BY t.transaction_id)        AS transaction_key,
       t.transaction_id,
       p.project_key,
       COALESCE(ev.employee_key, cur.employee_key)          AS employee_key,
       v.vendor_key,
       CAST(strftime(t.transaction_date, '%Y%m%d') AS INTEGER) AS date_key,
       t.amount_aed                                         AS amount,
       t.amount IS NULL                                     AS amount_is_missing,
       t.category,
       t.payment_status,
       t.currency,
       t.invoice_ref,
       t.is_approved
FROM stg_transactions t
JOIN dim_project p ON p.project_id = t.project_id
JOIN dim_vendor  v ON v.vendor_id  = t.vendor_id
LEFT JOIN dim_employee cur ON cur.employee_id = t.approved_by AND cur.is_current
LEFT JOIN dim_employee ev  ON ev.employee_id  = t.approved_by
                          AND t.transaction_date >= ev.valid_from AND t.transaction_date < ev.valid_to;

-- ---------------------------------------------------------------------------
-- Load reconciliation: row counts across the whole star schema. Expected:
-- 500 projects, 2,231 employee versions for 1,000 employees, 25 vendors,
-- 50,000 fact rows (one per cleaned transaction).
-- ---------------------------------------------------------------------------
SELECT (SELECT COUNT(*) FROM dim_project)                     AS dim_project,
       (SELECT COUNT(*) FROM dim_employee)                    AS dim_employee_versions,
       (SELECT COUNT(DISTINCT employee_id) FROM dim_employee) AS distinct_employees,
       (SELECT COUNT(*) FROM dim_vendor)                      AS dim_vendor,
       (SELECT COUNT(*) FROM bridge_employee_project)         AS bridge_rows,
       (SELECT COUNT(*) FROM fact_transactions)               AS fact_rows;
