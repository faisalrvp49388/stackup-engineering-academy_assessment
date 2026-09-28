# Pillar 2 — SQL & Visualization

- `etl_full.py` — Task 2.2: loads/enriches transactions, reuses Pillar 1 for projects/employees and `04_infrastructure/dq_framework.py` for the checks.
- `queries.sql` — Task 2.1, six business questions (Q2 and Q3 legitimately return 0 rows; see comments).
- `query_optimization.sql` — Task 2.3 on PostgreSQL 15 (psql script): original query, captured EXPLAIN ANALYZE + bottleneck analysis, rewrite before/after covering indexes, benchmark (~3.2x) and a bonus test of why INCLUDE matters.
- `query_optimization_duckdb.sql` — the same original / rewritten queries and key-only indexes on DuckDB (run by `run_sql.py --bench`); no meaningful difference at this size.
- `run_sql.py` — builds the DuckDB warehouse and runs the SQL files (`--bench` adds the DuckDB benchmark).
- `run_optimization_postgres.py` — reproduces Task 2.3's PostgreSQL 15 benchmark for real: spins up a
  disposable `postgres:15` container, loads the raw datasets, and runs `query_optimization.sql`'s
  ORIGINAL/REWRITTEN queries and indexes (parsed from that file, not duplicated) before/after indexing.
- `build_dashboard_mockup.py` — Task 2.4 mockup PDF using real numbers.

Outputs: `outputs/results/mohammed_faisal/02_sql_and_viz/` (clean CSVs, `pipeline_summary.txt`, `dq_report_*.md`, `sql_results.txt`, `dashboard_mockup.pdf`).
- `notebooks/warehouse_explorer.ipynb` — live queries on the warehouse (SCD2 history, Q1/Q3/Q5/Q6).
