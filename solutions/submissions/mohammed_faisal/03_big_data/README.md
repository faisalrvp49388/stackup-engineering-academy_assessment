# Pillar 3 — Big Data

- `spark_pipeline.py` + `Dockerfile.spark` — Task 3.1: five Parquet tables from 12 monthly event files; timing baseline printed at the end.
- `kafka_streaming.py` — Task 3.2: topics, producer (50 ms default, `--delay` to change), consumer, Critical-escalation forwarding, `summary.json`.
- `airflow_dag.py` + `deploy_dag.ps1` — Task 3.3: `presight_etl_pipeline`, daily 06:00 Asia/Dubai, DQ gate, XCom report. Verified in the compose Airflow (all tasks green); mounted via `docker-compose.override.yml`.

Outputs: `outputs/results/mohammed_faisal/03_big_data/{spark,kafka}/`.
- `notebooks/big_data_explorer.ipynb` — reads the Spark Parquet tables via DuckDB and the Kafka summary.
