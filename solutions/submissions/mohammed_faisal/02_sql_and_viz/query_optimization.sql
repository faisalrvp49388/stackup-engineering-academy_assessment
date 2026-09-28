-- =============================================================================
-- StackUp Engineering Academy — Data Engineering Assessment
-- Solution File: query_optimization.sql
-- Pillar: SQL & Data Visualization (Task 2.3) — Query optimisation
-- Author: Mohammed Faisal | Engine: PostgreSQL 15
-- =============================================================================
--
-- SCENARIO
-- --------
-- A query used on the finance dashboard, run hundreds of times a day, is
-- slow. Diagnose it, rewrite it, and add indexes where they'd help in
-- production.
--
-- This was first measured on DuckDB (query_optimization_duckdb.sql), where
-- the rewrite and indexes showed no real difference — both queries land at
-- ~6-8 ms, because DuckDB is columnar with zone maps and vectorised hash
-- joins and doesn't need a B-tree to skip rows cheaply. That result was not
-- left as reasoning only: it's tested here for real against PostgreSQL, a
-- genuine row-store and the kind of engine the production indexes are
-- written for. Every plan below is a real captured run on PostgreSQL 15.19.
--
-- HOW TO RUN
-- ----------
-- Easiest — one command, fully automatic:
--   python solutions/submissions/mohammed_faisal/02_sql_and_viz/run_optimization_postgres.py
-- It spins up a disposable postgres:15 container (presight-pg-bench, NOT
-- docker-compose.yml's presight-postgres, which serves Airflow and is never
-- touched), stages the raw files, and parses the setup block, queries and
-- indexes straight out of this file — nothing is duplicated in Python.
--
-- Manually — COPY below is server-side, so the files need to exist inside
-- the container first (transactions.json flattened to CSV; the runner does
-- this for you):
--   docker run -d --name presight-pg-bench -p 5433:5432 -e POSTGRES_USER=presight -e POSTGRES_PASSWORD=presight123 -e POSTGRES_DB=presight_bench postgres:15
--   docker cp datasets/employees.csv presight-pg-bench:/tmp/employees.csv
--   docker cp datasets/projects.csv  presight-pg-bench:/tmp/projects.csv
--   docker cp <transactions.json flattened to CSV> presight-pg-bench:/tmp/transactions.csv
--   docker exec -i presight-pg-bench psql -U presight -d presight_bench -f - < solutions/submissions/mohammed_faisal/02_sql_and_viz/query_optimization.sql
--
-- Benchmark conditions: 50,000 transactions / 500 projects / 1,000
-- employees from the raw files, primary keys only, ANALYZE'd, warm cache.
-- Medians are over 15 runs; each EXPLAIN ANALYZE capture is one
-- representative run, so it sits a little off the median.
-- =============================================================================

\timing on

-- ---------------------------------------------------------------------------
-- Setup — plain tables matching the starter query's table names
-- ---------------------------------------------------------------------------
-- Loaded from the raw files, with primary keys only (what an operational
-- database would already have) and no secondary indexes.
DROP TABLE IF EXISTS employees, projects, transactions;

CREATE TABLE employees (
    employee_id       TEXT PRIMARY KEY,
    full_name         TEXT,
    email             TEXT,
    department        TEXT,
    role              TEXT,
    level             TEXT,
    hire_date         TEXT,      -- raw file has mixed date formats
    salary            INTEGER,
    manager_id        TEXT,
    region            TEXT,
    status            TEXT,
    years_experience  INTEGER
);

CREATE TABLE projects (
    project_id          TEXT PRIMARY KEY,
    project_name        TEXT,
    department          TEXT,
    status              TEXT,
    start_date          DATE,
    end_date            DATE,
    budget              NUMERIC,
    actual_cost         NUMERIC,
    project_manager_id  TEXT,
    priority            TEXT,
    region              TEXT
);

CREATE TABLE transactions (
    transaction_id    TEXT PRIMARY KEY,
    project_id        TEXT,
    vendor_id         TEXT,
    vendor_name       TEXT,
    category          TEXT,
    amount            NUMERIC,
    currency          TEXT,
    transaction_date  DATE,
    approved_by       TEXT,
    payment_status    TEXT,
    invoice_ref       TEXT,
    notes             TEXT
);

COPY employees    FROM '/tmp/employees.csv'    WITH (FORMAT csv, HEADER true);
COPY projects     FROM '/tmp/projects.csv'     WITH (FORMAT csv, HEADER true);
COPY transactions FROM '/tmp/transactions.csv' WITH (FORMAT csv, HEADER true);

ANALYZE employees;
ANALYZE projects;
ANALYZE transactions;


-- ---------------------------------------------------------------------------
-- ORIGINAL QUERY (unmodified from the starter file)
-- ---------------------------------------------------------------------------
EXPLAIN ANALYZE
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
-- EXPLAIN ANALYZE output — original query, real captured run (primary keys
-- only; median of 15 runs: 11.09 ms)
-- ---------------------------------------------------------------------------
--                                                             QUERY PLAN
-- ------------------------------------------------------------------------------------------------------------------------------------
--  Sort  (cost=3336.08..3339.69 rows=1445 width=118) (actual time=10.795..10.821 rows=923 loops=1)
--    Sort Key: e.department, t.amount DESC
--    Sort Method: quicksort  Memory: 178kB
--    Buffers: shared hit=1825
--    InitPlan 1 (returns $0)
--      ->  Aggregate  (cost=1542.68..1542.69 rows=1 width=32) (actual time=4.763..4.764 rows=1 loops=1)
--            Buffers: shared hit=895
--            ->  Seq Scan on transactions  (cost=0.00..1520.00 rows=9072 width=6) (actual time=0.002..4.259 rows=8927 loops=1)
--                  Filter: (payment_status = 'Pending'::text)
--                  Rows Removed by Filter: 41073
--                  Buffers: shared hit=895
--    ->  Hash Join  (cost=60.74..1717.55 rows=1445 width=118) (actual time=5.140..9.975 rows=923 loops=1)
--          Hash Cond: (p.project_manager_id = e.employee_id)
--          Buffers: shared hit=1819
--          ->  Hash Join  (cost=18.24..1671.24 rows=1445 width=89) (actual time=4.896..9.576 rows=923 loops=1)
--                Hash Cond: (t.project_id = p.project_id)
--                Buffers: shared hit=1799
--                ->  Seq Scan on transactions t  (cost=0.00..1645.00 rows=3024 width=34) (actual time=4.772..9.242 rows=2127 loops=1)
--                      Filter: ((amount > $0) AND (payment_status = 'Pending'::text))
--                      Rows Removed by Filter: 47873
--                      Buffers: shared hit=1790
--                ->  Hash  (cost=15.25..15.25 rows=239 width=71) (actual time=0.102..0.103 rows=239 loops=1)
--                      Buckets: 1024  Batches: 1  Memory Usage: 33kB
--                      Buffers: shared hit=9
--                      ->  Seq Scan on projects p  (cost=0.00..15.25 rows=239 width=71) (actual time=0.007..0.058 rows=239 loops=1)
--                            Filter: (status <> ALL ('{Completed,"On Hold"}'::text[]))
--                            Rows Removed by Filter: 261
--                            Buffers: shared hit=9
--          ->  Hash  (cost=30.00..30.00 rows=1000 width=45) (actual time=0.230..0.231 rows=1000 loops=1)
--                Buckets: 1024  Batches: 1  Memory Usage: 86kB
--                Buffers: shared hit=20
--                ->  Seq Scan on employees e  (cost=0.00..30.00 rows=1000 width=45) (actual time=0.007..0.078 rows=1000 loops=1)
--                      Buffers: shared hit=20
--  Planning Time: 1.003 ms
--  Execution Time: 10.938 ms
--
-- Summary — where this 10.9ms run actually goes (self-time per step, approx.;
-- the main scan's 9.24ms includes the 4.76ms InitPlan it waits on first):
--   Step                          | Scan Type | Rows In -> Out  | Time     | %
--   ------------------------------+-----------+-----------------+----------+-----
--   AVG(amount) InitPlan          | Seq Scan  | 50,000 -> 8,927 | 4.76 ms  | 44%
--   transactions filter (t)       | Seq Scan  | 50,000 -> 2,127 | 4.48 ms  | 41%
--   Hash Join x2 + Sort + small   | (memory)  | -> 923 rows     | 1.70 ms  | 15%
--   ------------------------------+-----------+-----------------+----------+-----
--   TOTAL                                                        10.94 ms | 100%
--   Buffers: 1,825 pages read — 1,790 of them from transactions (895 x 2).
--
-- Bottleneck analysis:
--   Joins used: two Hash Joins — transactions/projects on project_id, then
--   that result/employees on project_manager_id = employee_id. Both build
--   tiny in-memory hash tables (239 projects, 1,000 employees) and together
--   cost well under a millisecond. The joins are NOT the bottleneck.
--
--   What IS consuming the time: two full Seq Scans on transactions (50,000
--   rows each) — ~85% of the run. The table is read TWICE:
--     1. InitPlan 1 (~4.8ms) — reads all 50,000 rows, discards 41,073, just
--        to compute one number: the average Pending amount.
--     2. The main Seq Scan on transactions t (~4.5ms) — reads all 50,000
--        rows AGAIN, filtering payment_status AND amount > $0, discarding
--        47,873 to keep the 2,127 that matter.
--   Neither scan has an index to seek the ~18% Pending rows directly.
--   projects / employees (9 / 20 pages) are too small for an index to beat
--   a Seq Scan, and the final Sort is trivial (178 kB quicksort).
--
--   Correlated subquery re-executed per row? NO. The subquery references
--   nothing from the outer query, so PostgreSQL evaluates it ONCE as an
--   InitPlan (loops=1). Its cost is the extra full scan, not per-row
--   execution. An engine that doesn't hoist uncorrelated subqueries would
--   run it per row, so not depending on that optimisation is still good
--   practice.
--
--   Implicit "FROM a, b, c" joins are turned into hash joins by the
--   planner, so they perform like explicit JOINs here — but they hide
--   intent, and a forgotten predicate silently becomes a cross join.


-- ---------------------------------------------------------------------------
-- REWRITTEN QUERY — before indexes
-- ---------------------------------------------------------------------------
-- 5 changes: (1) scalar subquery -> CTE, computed once; (2) projects
-- filtered in a CTE BEFORE the join; (3) explicit column list, no SELECT *
-- — which is what later lets a covering index answer the query without
-- the heap; (4) implicit FROM A,B,C -> explicit JOIN...ON (a dropped WHERE
-- on a comma-join silently becomes a cartesian product); (5) predicates
-- applied directly on the big table.
EXPLAIN ANALYZE
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
-- EXPLAIN ANALYZE output — rewritten query, before indexes, real captured
-- run (median of 15 runs: 11.52 ms)
-- ---------------------------------------------------------------------------
--                                                                QUERY PLAN
-- ------------------------------------------------------------------------------------------------------------------------------------------
--  Sort  (cost=3281.30..3284.91 rows=1445 width=118) (actual time=13.962..13.990 rows=923 loops=1)
--    Sort Key: e.department, t.amount DESC
--    Sort Method: quicksort  Memory: 178kB
--    Buffers: shared hit=1825
--    ->  Hash Join  (cost=1603.42..3205.46 rows=1445 width=118) (actual time=7.639..12.992 rows=923 loops=1)
--          Hash Cond: (projects.project_manager_id = e.employee_id)
--          Buffers: shared hit=1819
--          ->  Nested Loop  (cost=1560.92..3159.15 rows=1445 width=89) (actual time=7.087..12.242 rows=923 loops=1)
--                Join Filter: (t.amount > (avg(transactions.amount)))
--                Rows Removed by Join Filter: 2990
--                Buffers: shared hit=1799
--                ->  Aggregate  (cost=1542.68..1542.69 rows=1 width=32) (actual time=6.956..6.958 rows=1 loops=1)
--                      Buffers: shared hit=895
--                      ->  Seq Scan on transactions  (cost=0.00..1520.00 rows=9072 width=6) (actual time=0.005..6.465 rows=8927 loops=1)
--                            Filter: (payment_status = 'Pending'::text)
--                            Rows Removed by Filter: 41073
--                            Buffers: shared hit=895
--                ->  Hash Join  (cost=18.24..1562.25 rows=4336 width=89) (actual time=0.128..5.004 rows=3913 loops=1)
--                      Hash Cond: (t.project_id = projects.project_id)
--                      Buffers: shared hit=904
--                      ->  Seq Scan on transactions t  (cost=0.00..1520.00 rows=9072 width=34) (actual time=0.005..3.897 rows=8927 loops=1)
--                            Filter: (payment_status = 'Pending'::text)
--                            Rows Removed by Filter: 41073
--                            Buffers: shared hit=895
--                      ->  Hash  (cost=15.25..15.25 rows=239 width=71) (actual time=0.099..0.100 rows=239 loops=1)
--                            Buckets: 1024  Batches: 1  Memory Usage: 33kB
--                            Buffers: shared hit=9
--                            ->  Seq Scan on projects  (cost=0.00..15.25 rows=239 width=71) (actual time=0.004..0.050 rows=239 loops=1)
--                                  Filter: (status <> ALL ('{Completed,"On Hold"}'::text[]))
--                                  Rows Removed by Filter: 261
--                                  Buffers: shared hit=9
--          ->  Hash  (cost=30.00..30.00 rows=1000 width=45) (actual time=0.537..0.538 rows=1000 loops=1)
--                Buckets: 1024  Batches: 1  Memory Usage: 86kB
--                Buffers: shared hit=20
--                ->  Seq Scan on employees e  (cost=0.00..30.00 rows=1000 width=45) (actual time=0.009..0.347 rows=1000 loops=1)
--                      Buffers: shared hit=20
--  Planning Time: 0.890 ms
--  Execution Time: 14.130 ms
--
-- Summary — where this 14.1ms run actually goes (self-time per step, approx.):
--   Step                          | Scan Type | Rows In -> Out  | Time     | %
--   ------------------------------+-----------+-----------------+----------+-----
--   AVG(amount) CTE               | Seq Scan  | 50,000 -> 8,927 | 6.96 ms  | 49%
--   transactions (t) + join proj. | Seq Scan  | 50,000 -> 3,913 | 5.00 ms  | 35%
--   Nested Loop + Hash Join + Sort| (memory)  | -> 923 rows     | 2.17 ms  | 15%
--   ------------------------------+-----------+-----------------+----------+-----
--   TOTAL                                                        14.13 ms | 100%
--
-- Analysis: same bottleneck, slightly different shape. The CROSS JOIN to
-- the one-row CTE is planned as a Nested Loop + Join Filter instead of an
-- InitPlan, so amount > avg is checked AFTER the project join (3,913 rows
-- joined, 2,990 thrown away) rather than inside the scan. Still two full
-- Seq Scans on transactions, still 1,825 buffers. On its own the rewrite
-- is performance-neutral (11.09 -> 11.52 ms median, within noise): it is a
-- correctness and readability change, shaped so the covering index below
-- can answer it — that is where the real gain comes from.


-- ---------------------------------------------------------------------------
-- INDEXES — for the production deployment this query actually runs in
-- ---------------------------------------------------------------------------
-- Covers WHERE payment_status = 'Pending' (both the AVG() and the main
-- scan) and the project_id join. payment_status first — equality predicate
-- that narrows to the ~18% Pending rows — then project_id (join key).
-- amount / category / transaction_date are INCLUDEd (payload only, keeps
-- the B-tree keys small) so both transactions scans become Index Only
-- Scans that never touch the heap.
CREATE INDEX idx_transactions_status_project
    ON transactions (payment_status, project_id)
    INCLUDE (amount, category, transaction_date);

-- Covers the status filter + join to employees on the projects side.
-- status first (filter), then project_manager_id (join key); display
-- columns INCLUDEd. At 500 rows (9 pages) PostgreSQL still prefers a Seq
-- Scan, so this one only pays off as the table grows.
CREATE INDEX idx_projects_status_manager
    ON projects (status, project_manager_id)
    INCLUDE (project_name, budget, actual_cost);

-- employees.employee_id is already the primary key — no new index needed.

VACUUM ANALYZE transactions;
VACUUM ANALYZE projects;


-- ---------------------------------------------------------------------------
-- REWRITTEN QUERY — after indexes
-- ---------------------------------------------------------------------------
EXPLAIN ANALYZE
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
-- EXPLAIN ANALYZE output — rewritten query, after indexes, real captured run
-- (median of 15 runs: 3.43 ms)
-- ---------------------------------------------------------------------------
--                                                                                      QUERY PLAN
-- --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--  Sort  (cost=1061.95..1065.49 rows=1418 width=118) (actual time=3.956..3.982 rows=923 loops=1)
--    Sort Key: e.department, t.amount DESC
--    Sort Method: quicksort  Memory: 178kB
--    Buffers: shared hit=173
--    ->  Hash Join  (cost=495.53..987.72 rows=1418 width=118) (actual time=1.476..3.138 rows=923 loops=1)
--          Hash Cond: (projects.project_manager_id = e.employee_id)
--          Buffers: shared hit=167
--          ->  Nested Loop  (cost=453.03..941.49 rows=1418 width=89) (actual time=1.246..2.772 rows=923 loops=1)
--                Join Filter: (t.amount > (avg(transactions.amount)))
--                Rows Removed by Join Filter: 2990
--                Buffers: shared hit=147
--                ->  Aggregate  (cost=434.38..434.39 rows=1 width=32) (actual time=1.055..1.056 rows=1 loops=1)
--                      Buffers: shared hit=69
--                      ->  Index Only Scan using idx_transactions_status_project on transactions  (cost=0.41..412.13 rows=8898 width=6) (actual time=0.029..0.619 rows=8927 loops=1)
--                            Index Cond: (payment_status = 'Pending'::text)
--                            Heap Fetches: 0
--                            Buffers: shared hit=69
--                ->  Hash Join  (cost=18.65..453.93 rows=4253 width=89) (actual time=0.187..1.465 rows=3913 loops=1)
--                      Hash Cond: (t.project_id = projects.project_id)
--                      Buffers: shared hit=78
--                      ->  Index Only Scan using idx_transactions_status_project on transactions t  (cost=0.41..412.13 rows=8898 width=34) (actual time=0.015..0.572 rows=8927 loops=1)
--                            Index Cond: (payment_status = 'Pending'::text)
--                            Heap Fetches: 0
--                            Buffers: shared hit=69
--                      ->  Hash  (cost=15.25..15.25 rows=239 width=71) (actual time=0.152..0.152 rows=239 loops=1)
--                            Buckets: 1024  Batches: 1  Memory Usage: 33kB
--                            Buffers: shared hit=9
--                            ->  Seq Scan on projects  (cost=0.00..15.25 rows=239 width=71) (actual time=0.004..0.101 rows=239 loops=1)
--                                  Filter: (status <> ALL ('{Completed,"On Hold"}'::text[]))
--                                  Rows Removed by Filter: 261
--                                  Buffers: shared hit=9
--          ->  Hash  (cost=30.00..30.00 rows=1000 width=45) (actual time=0.219..0.219 rows=1000 loops=1)
--                Buckets: 1024  Batches: 1  Memory Usage: 86kB
--                Buffers: shared hit=20
--                ->  Seq Scan on employees e  (cost=0.00..30.00 rows=1000 width=45) (actual time=0.007..0.080 rows=1000 loops=1)
--                      Buffers: shared hit=20
--  Planning Time: 0.947 ms
--  Execution Time: 4.095 ms
--
-- What improved (14.13ms -> 4.10ms single run; 11.52 -> 3.43ms median) —
-- same Nested Loop / Hash Join / Sort shape before and after, just much
-- cheaper leaf scans:
--   Step               | Before (Seq Scan)     | After (Index Only Scan)
--   -------------------+-----------------------+--------------------------------------
--   AVG(amount) scan   | 6.47 ms, 895 buffers  | 0.62 ms, 69 buffers, Heap Fetches: 0
--   Join-side scan (t) | 3.90 ms, 895 buffers  | 0.57 ms, 69 buffers, Heap Fetches: 0
--   Whole plan buffers | 1,825                 | 173  (~10.5x fewer pages read)
--   -------------------+-----------------------+--------------------------------------
--   projects stays a Seq Scan (239 of 500 rows on a 9-page table — cheaper
--   than any index), exactly as predicted above.
--
-- Benchmark — PostgreSQL 15.19, median of 15 runs, same data throughout:
--   Variant                               | Median   | Buffers hit
--   --------------------------------------+----------+------------
--   original,  primary keys only          | 11.09 ms | 1,825
--   rewritten, primary keys only          | 11.52 ms | 1,825
--   original,  with the indexes above     |  3.49 ms |   173
--   rewritten, with the indexes above     |  3.43 ms |   173
--   --------------------------------------+----------+------------
--   SPEEDUP: original (no indexes) -> rewritten + indexes
--            = 11.09 ms -> 3.43 ms = ~3.2x faster, ~10.5x fewer pages read.
--
-- Honest notes:
--   * With the covering index the ORIGINAL text reaches the same ~3.5 ms,
--     because PostgreSQL already hoists the uncorrelated subquery into an
--     InitPlan. The index is the performance fix; the rewrite is the
--     maintainability fix.
--   * The brief's 10x is not reachable on 50K rows that fit in memory. The
--     gap widens with table size — a full scan is O(table), the index-only
--     scan is O(matching rows).




-- ---------------------------------------------------------------------------
-- BONUS — do the INCLUDE columns matter, or would a plain key-only index
-- do? Tested, they matter.
-- ---------------------------------------------------------------------------
-- Same data and queries, with the two indexes above created WITHOUT their
-- INCLUDE clause (key columns only):
--   CREATE INDEX idx_transactions_status_project ON transactions (payment_status, project_id);
--   CREATE INDEX idx_projects_status_manager ON projects (status, project_manager_id);
--
-- Real captured plan fragment (rewritten query, key-only indexes):
--   ->  Aggregate  (actual time=3.428..3.430 rows=1 loops=1)
--         Buffers: shared hit=906
--         ->  Bitmap Heap Scan on transactions  (actual time=0.340..2.559 rows=8927 loops=1)
--               Recheck Cond: (payment_status = 'Pending'::text)
--               Heap Blocks: exact=895
--               ->  Bitmap Index Scan on idx_transactions_status_project  (actual time=0.275..0.276 rows=8927 loops=1)
--                     Index Cond: (payment_status = 'Pending'::text)
--   ->  Bitmap Heap Scan on transactions t  (actual time=0.320..1.715 rows=8927 loops=1)
--         Recheck Cond: (payment_status = 'Pending'::text)
--         Heap Blocks: exact=895
--   Execution Time: 8.275 ms       (Buffers: shared hit=1847)
--
--   Variant                               | Median   | Buffers hit
--   --------------------------------------+----------+------------
--   original,  key-only indexes           |  5.45 ms | 1,847
--   rewritten, key-only indexes           |  5.91 ms | 1,847
--   rewritten, covering (INCLUDE) indexes |  3.43 ms |   173
--   --------------------------------------+----------+------------
--
-- Why: the key-only index turns the Seq Scans into Bitmap Index Scans, but
-- every one of the 8,927 Pending rows still has to be fetched from the
-- heap for amount / category / transaction_date — Heap Blocks: exact=895
-- is the whole table — so buffers don't drop at all and time only halves.
-- With INCLUDE those columns live in the index leaf pages, the heap is
-- never touched (Heap Fetches: 0), and reads fall ~10x. That's the
-- difference between ~1.9x and ~3.2x.
--
-- Trade-off (why this isn't free):
--   The INCLUDE payload makes idx_transactions_status_project wider
--   (~2 MB on 50K rows), and every INSERT/UPDATE on transactions — likely
--   the fastest-growing table in the system — now maintains one more
--   B-tree (~5-10% slower bulk loads). This query runs hundreds of times a
--   day on an append-mostly table, so trading a small write overhead for
--   ~3x faster, ~10x cheaper reads is the right call.
--   idx_projects_status_manager is near-free: projects is small and rarely
--   written.
