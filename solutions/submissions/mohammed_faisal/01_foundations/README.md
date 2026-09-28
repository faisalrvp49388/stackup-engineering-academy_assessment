# Pillar 1 — Foundations

- `etl_pipeline.py` — Tasks 1.1 (projects) and 1.3 (employees quality) plus the shared output writer; imported by `02_sql_and_viz/etl_full.py`, `04_infrastructure/dq_framework.py` and the Airflow DAG.
- `data_model.sql` — Task 1.2: star schema DDL (6 tables), loads, SCD2 `dim_employee` build and validation queries (no duplicate currents, no overlaps, no gaps). Loads `dim_project`/`dim_employee` from this pillar's clean CSVs and `fact_transactions`/`dim_vendor` from Task 2.2's `transactions_clean.csv` (`02_sql_and_viz/etl_full.py` must run first).

Inputs: `datasets/projects.csv`, `employees.csv`, `employees_salary_history.csv` (plus `02_sql_and_viz/transactions_clean.csv` for `data_model.sql`).
Outputs: `outputs/results/mohammed_faisal/01_foundations/` (`projects_clean.csv`, `employees_clean.csv`, `employees_quality_summary.json`, `pipeline_summary.txt`); warehouse in `outputs/presight_warehouse.duckdb`.
Run: see [HOW_TO_RUN.md](../HOW_TO_RUN.md).
- `notebooks/foundations_explorer.ipynb` — real before/after tables for every fix.
