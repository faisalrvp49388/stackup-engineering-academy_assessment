# syntax=docker/dockerfile:1
# Task 4.1 — containerised ETL (the Task 2.2 pipeline).
#   docker build -t presight-etl .
#   docker run --rm -v "$(pwd)/outputs:/app/outputs" presight-etl
# A copy lives in solutions/submissions/mohammed_faisal/04_infrastructure/ for visibility; this root
# file is the build source (the build context must contain solutions/ and datasets/).

# ---- Stage 1: install dependencies into an isolated prefix -------------------------
FROM python:3.11-slim AS builder
WORKDIR /build
COPY solutions/submissions/mohammed_faisal/04_infrastructure/requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ---- Stage 2: slim runtime image ---------------------------------------------------
FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DATA_DIR=/app/datasets \
    OUTPUT_DIR=/app/outputs

WORKDIR /app
COPY --from=builder /install /usr/local
COPY solutions/submissions/mohammed_faisal/01_foundations/etl_pipeline.py ./solutions/submissions/mohammed_faisal/01_foundations/
COPY solutions/submissions/mohammed_faisal/02_sql_and_viz/etl_full.py ./solutions/submissions/mohammed_faisal/02_sql_and_viz/
COPY solutions/submissions/mohammed_faisal/04_infrastructure/dq_framework.py ./solutions/submissions/mohammed_faisal/04_infrastructure/
COPY datasets/projects.csv datasets/employees.csv datasets/employees_salary_history.csv datasets/transactions.json ./datasets/

VOLUME ["/app/outputs"]
ENTRYPOINT ["python", "solutions/submissions/mohammed_faisal/02_sql_and_viz/etl_full.py"]
