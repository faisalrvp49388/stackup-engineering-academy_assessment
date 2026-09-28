# Presight Analytics Platform — Data Engineering Assessment

*Author: Mohammed Faisal*

Run instructions: [HOW_TO_RUN.md](HOW_TO_RUN.md) · one-line commands: [QUICK_RUN.md](QUICK_RUN.md)

## Repository layout

```
solutions/submissions/mohammed_faisal/
├── 01_foundations/
│   ├── etl_pipeline.py        # Tasks 1.1 + 1.3 (projects, employees quality) + shared output writer; imported by the other pillars
│   ├── data_model.sql         # Task 1.2 — star schema + SCD2 build + validation queries
│   └── README.md
├── 02_sql_and_viz/
│   ├── etl_full.py            # Task 2.2 — transactions ETL + full pipeline (Docker + Airflow run this code)
│   ├── queries.sql            # Task 2.1 — six business questions
│   ├── query_optimization.sql # Task 2.3 — PostgreSQL: original / rewrite / indexes / benchmarks
│   ├── query_optimization_duckdb.sql # Task 2.3 — same queries on DuckDB (run_sql.py --bench)
│   ├── run_sql.py             # builds the warehouse and runs the three SQL files
│   ├── build_dashboard_mockup.py  # Task 2.4 — PDF dashboard from warehouse numbers
│   └── README.md
├── 03_big_data/
│   ├── spark_pipeline.py, Dockerfile.spark   # Task 3.1 (runs in Docker)
│   ├── kafka_streaming.py                    # Task 3.2
│   ├── airflow_dag.py, deploy_dag.ps1, .airflowignore   # Task 3.3 (mounted into Airflow by docker-compose.override.yml)
│   └── README.md
├── 04_infrastructure/
│   ├── data_governance.md     # Task 4.2
│   ├── dq_framework.py        # Task 4.3 — configurable DQ framework (DQ_CONFIG, checks, gate, reports)
│   ├── Dockerfile, requirements.txt, requirements-dev.txt   # Task 4.1 (the build source is the root Dockerfile)
│   └── README.md
├── */notebooks/               # foundations_explorer, warehouse_explorer, big_data_explorer (executed, outputs saved)
├── tests/test_etl.py          # 6 unit tests
├── Presight_Data_Project.pptx # 9-slide walkthrough deck, plain-language, own design (not modelled
│                              # on any other submission); includes the Task 1.2 star-schema
│                              # diagram as its own slide, built from native shapes, not an image
├── HOW_TO_RUN.md, QUICK_RUN.md, run_all.ps1
└── SUBMISSION_NOTES.md
outputs/results/mohammed_faisal/{01..04}_*/   # generated results, same pillar folders
```

`starter_files/` is untouched.

## Key decisions

**Data quality (Task 1.3).** Issues found by vectorised detection: 10 missing emails (derived as `first.last@presight.ai`,
uniqueness guaranteed); 8 unparseable hire dates (`-999`, `99999-01-01`) set to NULL — they could not be recovered from the
salary history, so those employees are flagged `active_without_hire_date_REVIEW`; 5 negative `years_experience` (level median);
3 juniors paid 66–76K (≈5× the level cap; scaled back where the result is in band); 5 manager ids pointing at a non-existent
`EMP0000` (NULL = top of the hierarchy). Nothing is silently dropped: every changed row carries a `dq_flags` value.

**Projects (Task 1.1).** Nulls in budget/actual_cost become 0 as required, but the derived variance and utilisation stay NULL
when an input was unknown instead of inventing numbers; `is_over_budget` is False when unknown. Output has 17 columns
(the 11 source columns plus the 4 requested metrics, `status_category` and `risk_level`), not the 15 quoted in the brief.

**SCD2 (Task 1.2).** Half-open periods `[valid_from, valid_to)`; `valid_to` of one version equals `valid_from` of the next, so the
Q6 self-join is a plain equality and overlap detection is unambiguous. One version per history row (2–5 versions per employee),
employees without history get a single row from `hire_date`. The newest version takes salary/role/level from the cleaned employee
master so the current row always agrees with `employees_clean.csv`. Validation: zero duplicate currents, overlaps and gaps
(2,231 versions for 1,000 employees).

**Fact table.** `employee_key` is the approver's SCD2 version valid on the transaction date (NULL for the ~5% unapproved rows);
null amounts are stored as 0 with `amount_is_missing = TRUE`.

**Query results worth knowing.** Q2 (managers with more than three active projects) and Q3 (vendors above 5% of spend) return
no rows on this dataset — the maximum is exactly 3 projects and 4.36% respectively; the queries are right, the data has no such cases.

**Query optimisation (Task 2.3).** Measured on PostgreSQL 15 with 50K rows: 11.1 ms → 3.4 ms (~3.2×, 10× fewer pages read) from the rewrite plus a covering
index. Almost all of the gain is the index; the rewrite alone is roughly neutral, and the original's subquery is not correlated
(evaluated once as an InitPlan). The brief's expected 10× is not reachable on data that fits in memory; the file documents this
with the real plans. DuckDB shows no difference at this size.

**DQ framework (Task 4.3).** All rules live in `DQ_CONFIG` (or a YAML file with the same shape); each check is a registered function,
so a new check is one registry line. Nine checks (six required + distribution, freshness, outliers); the Airflow gate reuses
`enforce_dq_gate` (in `dq_framework.py`) and fails only on completeness below 80% for non-optional columns.

**Spark (Task 3.1).** Explicit schema, wildcard load, 99,996 events; escalation matching pairs each raise with the first resolution
before the next raise on the same project. ~20 s on container-local disk (5,000 events/s); writing 365 date partitions through a
Windows bind mount is ~4× slower — an environment cost, not a code one.

## Verification status

| Item | Status |
|---|---|
| ETL (1.1, 1.3, 2.2), DQ framework (4.3) | Run; 6 unit tests pass |
| Star schema, SCD2 validation, six queries, benchmark | Run on DuckDB; benchmark plans on PostgreSQL 15 (Docker) |
| Spark | Run in Docker |
| Kafka | Run against the local broker with `--delay 0`; the 50 ms default was not run to completion |
| Docker ETL | Built and run; ~3.4 s for 50K transactions, outputs appear in the mounted folder |
| Airflow DAG | Run in the compose Airflow 2.7.3: all 9 tasks succeeded (manual and scheduled run), schedule `0 6 * * *` Asia/Dubai. Found and fixed a pandas-version bug in `transform_projects` (np.select with nullable booleans). DQ-gate failure path covered by a unit test only, not by a failing DAG run |
| Notebooks | Three explorer notebooks executed end to end with outputs saved |
| Dashboard | PDF mockup with real numbers (no Power BI file) |
