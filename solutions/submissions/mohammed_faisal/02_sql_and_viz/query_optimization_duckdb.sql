-- =============================================================================
-- StackUp Engineering Academy — Data Engineering Assessment
-- Solution File: query_optimization_duckdb.sql
-- Pillar: SQL & Data Visualization (Task 2.3) — Query optimisation
-- Author: Mohammed Faisal | Engine: DuckDB
-- =============================================================================
--
-- PREREQUISITES
-- -------------
-- Uses plain employees/projects/transactions tables built from the raw
-- files, not data_model.sql's star schema — the setup block below loads
-- them. Run from the repo root (paths are repo-relative).
--
-- HOW TO USE
-- ----------
-- Runs as part of the full DuckDB build, after data_model.sql and
-- queries.sql, and benchmarks original vs rewritten (median of 15 runs):
--   python solutions/submissions/mohammed_faisal/02_sql_and_viz/run_sql.py --bench
-- Or run it directly in any SQL client (DuckDB CLI, DuckDB UI, SQLTools)
-- whose working directory is the repo root.
--
-- RESULT SUMMARY: at this scale (1,000 employees / 500 projects / 50,000
-- transactions), original and rewritten both run ~6-8ms on DuckDB — no
-- 10x+ speedup here, with or without the indexes. See the analysis below
-- for why, and query_optimization.sql for the same query tested on
-- PostgreSQL 15, where the indexes DO matter (11.09 -> 3.43 ms, ~3.2x).
-- =============================================================================


-- ---------------------------------------------------------------------------
-- Setup — plain unindexed tables matching the starter query's table names
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE employees    AS SELECT * FROM read_csv_auto('datasets/employees.csv');
CREATE OR REPLACE TABLE projects     AS SELECT * FROM read_csv_auto('datasets/projects.csv');
CREATE OR REPLACE TABLE transactions AS SELECT * FROM read_json_auto('datasets/transactions.json');


-- ---------------------------------------------------------------------------
-- ORIGINAL QUERY (unmodified from the starter file)
-- ---------------------------------------------------------------------------
SELECT
    e.full_name,
    e.department,
    e.role,
    p.project_name,
    p.status,
    p.budget,
    p.actual_cost,
    t.amount,
    t.category,
    t.payment_status,
    t.transaction_date
FROM employees e, projects p, transactions t
WHERE e.employee_id = p.project_manager_id
AND   p.project_id  = t.project_id
AND   p.status NOT IN ('Completed', 'On Hold')
AND   t.payment_status = 'Pending'
AND   t.amount > (
        SELECT AVG(amount)
        FROM transactions
        WHERE payment_status = 'Pending'
      )
ORDER BY e.department, t.amount DESC;


-- ---------------------------------------------------------------------------
-- DuckDB result — original query
-- ---------------------------------------------------------------------------
-- Median ~6-8 ms over 15 runs (run_sql.py --bench) -> 923 rows returned.
--
-- Bottleneck analysis:
--   1. DuckDB plans the comma-join as hash joins, exactly like explicit
--      JOINs, and evaluates the uncorrelated AVG() subquery once — so
--      neither the join syntax nor the subquery costs anything extra.
--   2. The two passes over transactions are columnar scans: only the
--      payment_status / amount / project_id columns are read, in
--      vectorised batches, with zone maps skipping row groups that can't
--      match. Reading 50,000 rows this way takes a few milliseconds.
--   3. There is no row-store "full table scan" penalty for an index to
--      remove — which is why the indexes below change nothing here, and
--      why the real index test lives in query_optimization.sql on
--      PostgreSQL.


-- ---------------------------------------------------------------------------
-- REWRITTEN QUERY
-- ---------------------------------------------------------------------------
-- Same rewrite as query_optimization.sql: scalar subquery -> CTE, projects
-- filtered before the join, explicit JOIN...ON, explicit column list.
WITH avg_pending AS (
    SELECT AVG(amount) AS avg_amount
    FROM transactions
    WHERE payment_status = 'Pending'
),
active_projects AS (
    SELECT project_id, project_name, status, budget, actual_cost, project_manager_id
    FROM projects
    WHERE status NOT IN ('Completed', 'On Hold')
)
SELECT
    e.full_name,
    e.department,
    e.role,
    p.project_name,
    p.status,
    p.budget,
    p.actual_cost,
    t.amount,
    t.category,
    t.payment_status,
    t.transaction_date
FROM transactions t
JOIN active_projects p
    ON p.project_id = t.project_id
JOIN employees e
    ON e.employee_id = p.project_manager_id
CROSS JOIN avg_pending a
WHERE t.payment_status = 'Pending'
  AND t.amount > a.avg_amount
ORDER BY e.department, t.amount DESC;


-- ---------------------------------------------------------------------------
-- DuckDB result — rewritten query
-- ---------------------------------------------------------------------------
-- Median ~6-8 ms over 15 runs -> 923 rows, identical result set.
-- Within run-to-run noise of the original (three sessions: 6.16 vs 7.09,
-- 6.35 vs 6.28 and 7.88 vs 8.11 ms, original vs rewritten).


-- ---------------------------------------------------------------------------
-- INDEXES
-- ---------------------------------------------------------------------------
-- DuckDB doesn't need these, but the task frames this as a production
-- query, so the key columns are created here too. DuckDB has no INCLUDE
-- clause, so these are the key-only forms of the PostgreSQL covering
-- indexes in query_optimization.sql.
CREATE INDEX idx_transactions_status_project ON transactions (payment_status, project_id);
CREATE INDEX idx_projects_status_manager ON projects (status, project_manager_id);

-- With these indexes: still ~6-8 ms, 923 rows — no change. DuckDB's ART
-- indexes are used for point lookups and constraint checks, not to speed
-- up an analytical scan + hash join like this one.
--
-- Where this WOULD show a real gain: a row-store at production volume.
-- Tested for real, not just reasoned about — see query_optimization.sql
-- (PostgreSQL 15: 11.09 ms -> 3.43 ms, ~3.2x, ~10.5x fewer pages read).
