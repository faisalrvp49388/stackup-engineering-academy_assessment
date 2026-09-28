# How to Run This Submission

All commands run from the **repo root**. One-line versions: [QUICK_RUN.md](QUICK_RUN.md). Design notes: [SUBMISSION_NOTES.md](SUBMISSION_NOTES.md).

## Prerequisites

- Python 3.11+ with `pip install pandas numpy pyyaml duckdb pyarrow matplotlib kafka-python pytest`
  (`kafka-python` 2.x and 3.x are both supported).
- Docker Desktop running (Spark job, Kafka, Airflow, and the ETL container).
- Nothing else: Spark runs in a container (Java is bundled), so no local Java/Hadoop setup is needed.

Credentials: Airflow UI http://localhost:8081 (`admin`/`admin`), Kafka UI http://localhost:8080, Postgres `presight`/`presight123`.

## Pillar 1 — Foundations

```powershell
python solutions/submissions/mohammed_faisal/01_foundations/etl_pipeline.py
```
Check: `projects_clean.csv` (500 rows, 17 columns), `employees_clean.csv` (1,000 rows), `employees_quality_summary.json`
and `pipeline_summary.txt` (detection report + rows affected per fix). The star schema (`01_foundations/data_model.sql`) is
built by `run_sql.py` in Pillar 2; `fact_transactions`/`dim_vendor` are loaded from Task 2.2's
`transactions_clean.csv`, so `etl_full.py` must run before `run_sql.py`.

## Pillar 2 — SQL & Visualization

```powershell
python solutions/submissions/mohammed_faisal/02_sql_and_viz/etl_full.py            # Task 2.2 (about 1 s)
python solutions/submissions/mohammed_faisal/02_sql_and_viz/run_sql.py --bench      # Tasks 1.2, 2.1, 2.3
python solutions/submissions/mohammed_faisal/02_sql_and_viz/build_dashboard_mockup.py   # Task 2.4
```
Check: the SCD2 validation queries (Q1, Q2, Q4, Q5) print zero rows and Q6 shows actual_rows = expected_rows; six business-question results are printed;
`dashboard_mockup.pdf` is written. The PostgreSQL benchmark quoted in `query_optimization.sql` was produced with
`postgres:15` in Docker; to reproduce those EXPLAIN ANALYZE plans and timings yourself:
```powershell
python solutions/submissions/mohammed_faisal/02_sql_and_viz/run_optimization_postgres.py    # needs Docker
```
Spins up a disposable Postgres container (separate from docker-compose.yml's, which serves Airflow),
loads the raw datasets, and runs the ORIGINAL/REWRITTEN queries and indexes straight out of
`query_optimization.sql` before/after indexing. Removes the container when done unless you pass `--keep`.

## Pillar 3 — Big Data

```powershell
# Spark (Docker)
docker build -f solutions/submissions/mohammed_faisal/03_big_data/Dockerfile.spark -t presight-spark .
docker run --rm -v "${PWD}/datasets:/app/datasets" -v "${PWD}/outputs/results/mohammed_faisal/03_big_data:/app/out" presight-spark

# Kafka
docker compose up -d zookeeper kafka
python solutions/submissions/mohammed_faisal/03_big_data/kafka_streaming.py --mode both          # ~7 min at 50 ms
python solutions/submissions/mohammed_faisal/03_big_data/kafka_streaming.py --mode both --delay 0 # ~15 s

# Airflow
docker compose up -d      # the override mounts the solution DAG; Airflow UI takes 1-2 min
.\solutions\submissions\mohammed_faisal\03_big_data\deploy_dag.ps1
docker exec presight-airflow-scheduler airflow dags trigger presight_etl_pipeline
```
Check: five Parquet folders in `outputs/results/mohammed_faisal/03_big_data/spark/` (`daily_event_volume` partitioned by
`event_date`, `escalation_log` by `severity`); `kafka/summary.json`; DAG `presight_etl_pipeline` in the UI, schedule `0 6 * * *` (Asia/Dubai).

## Pillar 4 — Infrastructure & Governance

```powershell
docker build -t presight-etl .
docker run --rm -v "${PWD}/outputs/results/mohammed_faisal/04_infrastructure:/app/outputs" presight-etl
python solutions/submissions/mohammed_faisal/04_infrastructure/dq_framework.py
docker compose run --rm etl        # alternative, via docker-compose.override.yml
```
The `Dockerfile` and `.dockerignore` are at the repo root (the build context needs `solutions/` and `datasets/`);
a copy of the Dockerfile sits in `04_infrastructure/` for visibility.

## Tests

```powershell
python -m pytest -q solutions/submissions/mohammed_faisal/tests
```
