# Quick Run Reference

Run everything from the **repo root**. Full details: [HOW_TO_RUN.md](HOW_TO_RUN.md)

**Setup once:** `pip install pandas numpy pyyaml duckdb pyarrow matplotlib kafka-python pytest`
(Spark and Airflow run inside Docker, so no local Java/Hadoop/Airflow is needed.)

| # | Task | Run via | Command | Input | What it does | Output |
|---|---|---|---|---|---|---|
| 0 | — | Terminal | `docker compose up -d` (starts Zookeeper, Kafka, Kafka UI, Postgres, Airflow webserver + scheduler; the Airflow UI takes about 1-2 min to come up. Kafka only: `docker compose up -d zookeeper kafka kafka-ui`) | — | Starts Kafka + Kafka UI + Airflow + Postgres | Kafka UI `localhost:8080`, Airflow UI `localhost:8081` (admin/admin) |
| 1 | 1.1 projects + 1.3 employees | Terminal | `python solutions/submissions/mohammed_faisal/01_foundations/etl_pipeline.py` | `datasets/projects.csv`, `employees.csv`, `employees_salary_history.csv` | Cleans projects (17 columns) and employees; logs the detection report and rows affected per fix | `outputs/results/mohammed_faisal/01_foundations/` (`projects_clean.csv`, `employees_clean.csv`, `employees_quality_summary.json`, `pipeline_summary.txt`) |
| 2 | 2.2 full ETL | Terminal | `python solutions/submissions/mohammed_faisal/02_sql_and_viz/etl_full.py` | projects, employees, transactions (+ salary history) | Cleans all datasets, enriches 50K transactions, runs the DQ checks, writes CSVs + `pipeline_summary.txt` + `dq_report_*.md` | `outputs/results/mohammed_faisal/02_sql_and_viz/` |
| 3 | 1.2 star schema + SCD2, 2.1 six queries, 2.3 optimisation | Terminal | `python solutions/submissions/mohammed_faisal/02_sql_and_viz/run_sql.py --bench` | Step 1's `projects_clean.csv`/`employees_clean.csv`, step 2's `transactions_clean.csv`, and `employees_salary_history.csv` | Builds the DuckDB warehouse (`data_model.sql`; `fact_transactions`/`dim_vendor` are built from step 2's cleaned transactions), runs the SCD2 validation queries, the six business questions (`queries.sql`) and the DuckDB optimisation benchmark (`query_optimization_duckdb.sql`) | printed + `outputs/presight_warehouse.duckdb` |
| 4 | 2.4 dashboard | Terminal | `python solutions/submissions/mohammed_faisal/02_sql_and_viz/build_dashboard_mockup.py` | the warehouse from step 3 | Builds the annotated dashboard mockup with real numbers | `outputs/results/mohammed_faisal/02_sql_and_viz/dashboard_mockup.pdf` |
| — | 2.3 optimisation on real Postgres | Terminal (Docker) | `python solutions/submissions/mohammed_faisal/02_sql_and_viz/run_optimization_postgres.py` | raw datasets | Disposable `postgres:15` container; loads the data and reproduces `query_optimization.sql`'s EXPLAIN ANALYZE plans/timings before and after indexing | printed (container removed after, unless `--keep`) |
| 5 | 3.1 Spark | Terminal (Docker) | `docker build -f solutions/submissions/mohammed_faisal/03_big_data/Dockerfile.spark -t presight-spark .`<br>`docker run --rm -v "${PWD}/datasets:/app/datasets" -v "${PWD}/outputs/results/mohammed_faisal/03_big_data:/app/out" presight-spark` | `datasets/events_stream/*.jsonl` | Processes ~100K events into 5 Parquet tables and prints the timing baseline | `outputs/results/mohammed_faisal/03_big_data/spark/` |
| 6 | 3.2 Kafka | Terminal | `python solutions/submissions/mohammed_faisal/03_big_data/kafka_streaming.py --mode both` (add `--delay 0` for a 15-second run) | `events_2025_01.jsonl` | Produces 8,333 events, consumes them, forwards Critical escalations, writes the summary | `outputs/results/mohammed_faisal/03_big_data/kafka/summary.json` |
| 7 | 3.3 Airflow | Terminal | `.\solutions\submissions\mohammed_faisal\03_big_data\deploy_dag.ps1`<br>`docker exec presight-airflow-scheduler airflow dags trigger presight_etl_pipeline` | raw datasets | Serves the solution DAG (via `docker-compose.override.yml` mounts) and triggers a run (DQ gate → transform → load → report) | `outputs/pipeline_report_<date>.txt` |
| 8 | 4.1 Docker | Terminal | `docker build -t presight-etl .`<br>`docker run --rm -v "${PWD}/outputs/results/mohammed_faisal/04_infrastructure:/app/outputs" presight-etl` | raw datasets baked into the image | Runs the ETL inside a container | `outputs/results/mohammed_faisal/04_infrastructure/` |
| 9 | 4.3 DQ framework | Terminal | `python solutions/submissions/mohammed_faisal/04_infrastructure/dq_framework.py` | raw datasets | Runs all configurable checks, writes the markdown reports | `outputs/results/mohammed_faisal/04_infrastructure/dq_report_*.md` |
| — | 4.2 Governance | — (document) | — | — | Static document | `04_infrastructure/data_governance.md`, copy in `outputs/results/mohammed_faisal/04_infrastructure/data_governance_document.md` |
| — | Notebooks | VS Code / Jupyter | open `*/notebooks/*.ipynb` (repo root is auto-detected) | outputs of steps 1-6 | Explore before/after fixes, the warehouse and Spark/Kafka results | — |
| — | Tests | Terminal | `python -m pytest -q solutions/submissions/mohammed_faisal/tests` | — | 6 unit tests for ETL and DQ gate | — |

**Order:** 1 → 2 → 3 → 4 (step 3's warehouse needs step 2's `transactions_clean.csv`). Step 0 before 6 and 7. Steps 5, 8 and 9 are independent.

---

## Run everything end-to-end

```powershell
.\solutions\submissions\mohammed_faisal\run_all.ps1
```

Runs steps 0–9 in order. Skip switches: `-SkipSpark`, `-SkipKafka`, `-SkipAirflow`, `-SkipDocker`;
`-FastKafka` runs Kafka with no delay (about 15 s instead of ~7 min).

```powershell
.\solutions\submissions\mohammed_faisal\run_all.ps1 -SkipAirflow -FastKafka
```

> Verified: Airflow DAG run succeeded end to end (all 9 tasks green) and both UIs respond. The pipeline report lands in `outputs/`; copies are in `outputs/results/mohammed_faisal/03_big_data/`.
