# Makes Airflow serve this solution's DAG. docker-compose.override.yml mounts
# 03_big_data -> /opt/airflow/dags and 01_foundations -> /opt/airflow/etl, so "deploying" is
# just (re)creating the two Airflow containers and unpausing the DAG. Run from the repo root:
#   .\solutions\submissions\mohammed_faisal\03_big_data\deploy_dag.ps1
# Stop at the first error.
$ErrorActionPreference = "Stop"
# Recreate the Airflow containers so they pick up the mounts in docker-compose.override.yml.
docker compose up -d airflow-webserver airflow-scheduler
Write-Host "Waiting for the scheduler to register the DAG..."
# Wait (up to ~3 min) until the scheduler has parsed the DAG.
for ($i = 0; $i -lt 20; $i++) {
    $found = docker exec presight-airflow-scheduler airflow dags list 2>$null | Select-String "presight_etl_pipeline"
    if ($found) { break }
    Start-Sleep -Seconds 10
}
# New DAGs start paused; unpause so the schedule and manual triggers work.
docker exec presight-airflow-scheduler airflow dags unpause presight_etl_pipeline
Write-Host "Ready: http://localhost:8081 (admin / admin) -> presight_etl_pipeline" -ForegroundColor Green
