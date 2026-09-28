# Runs the whole project end-to-end (all 4 pillars) in the order documented in QUICK_RUN.md.
# Nothing here does anything QUICK_RUN.md doesn't already document.
#
# Run from the repo root:  .\solutions\submissions\mohammed_faisal\run_all.ps1
#
# Skip switches for re-runs:
#   -SkipSpark    skip Task 3.1 (Spark, runs in Docker)
#   -SkipKafka    skip Task 3.2 (Kafka; ~7 min at the task's 50 ms/message delay)
#   -SkipAirflow  skip Task 3.3 (Airflow deploy + trigger)
#   -SkipDocker   skip Task 4.1 (Docker build + run)
#   -FastKafka    run Kafka with no delay (~15 s) instead of the spec's 50 ms
#
# Example: .\solutions\submissions\mohammed_faisal\run_all.ps1 -SkipAirflow -FastKafka

param(
    [switch]$SkipSpark,
    [switch]$SkipKafka,
    [switch]$SkipAirflow,
    [switch]$SkipDocker,
    [switch]$FastKafka
)

$ErrorActionPreference = "Stop"
$PY  = if (Test-Path ".venv\Scripts\python.exe") { ".venv\Scripts\python.exe" } else { "python" }
$SRC = "solutions\submissions\mohammed_faisal"
$OUT = "outputs\results\mohammed_faisal"

function Step($n, $title) {
    Write-Host ""
    Write-Host "==================================================================" -ForegroundColor Cyan
    Write-Host "  Step $n -- $title" -ForegroundColor Cyan
    Write-Host "==================================================================" -ForegroundColor Cyan
}
function Run($exe) {                    # run a command and stop on a non-zero exit code
    & $exe @args
    if ($LASTEXITCODE -ne 0) { throw "Command failed ($LASTEXITCODE): $exe $args" }
}

$started = Get-Date

# ---------------------------------------------------------------------------
Step 0 "Start Docker services (Kafka, Airflow, Postgres) -- only what is needed"
if (-not $SkipKafka)   { Run docker compose up -d zookeeper kafka }
if (-not $SkipAirflow) { Run docker compose up -d }

# ---------------------------------------------------------------------------
# Pillar 1 -- Foundations
# ---------------------------------------------------------------------------
Step 1 "Clean projects/employees (1.1, 1.3) -> $OUT\01_foundations"
$env:OUTPUT_DIR = "$PWD\$OUT\01_foundations"
Run $PY "$SRC\01_foundations\etl_pipeline.py"
Remove-Item Env:OUTPUT_DIR

# ---------------------------------------------------------------------------
# Pillar 2 -- SQL & Visualization
# ---------------------------------------------------------------------------
Step 2 "Full ETL (2.2) -> $OUT\02_sql_and_viz"
Run $PY "$SRC\02_sql_and_viz\etl_full.py"

Step 3 "Star schema + SCD2 (1.2), six business questions (2.1), query optimisation (2.3)"
Run $PY "$SRC\02_sql_and_viz\run_sql.py" --bench

Step 4 "Dashboard mockup (2.4)"
Run $PY "$SRC\02_sql_and_viz\build_dashboard_mockup.py"

# ---------------------------------------------------------------------------
# Pillar 3 -- Big Data Processing
# ---------------------------------------------------------------------------
if (-not $SkipSpark) {
    Step 5 "Spark: process events at scale (3.1, in Docker)"
    Run docker build -f "$SRC\03_big_data\Dockerfile.spark" -t presight-spark .
    Run docker run --rm -v "${PWD}/datasets:/app/datasets" -v "${PWD}/$OUT/03_big_data:/app/out" presight-spark
} else { Write-Host "Skipping Step 5 (Spark)." -ForegroundColor DarkGray }

if (-not $SkipKafka) {
    Step 6 "Kafka: producer + consumer (3.2)"
    $delay = if ($FastKafka) { "0" } else { "0.05" }
    Run $PY "$SRC\03_big_data\kafka_streaming.py" --mode both --delay $delay
} else { Write-Host "Skipping Step 6 (Kafka)." -ForegroundColor DarkGray }

if (-not $SkipAirflow) {
    Step 7 "Airflow: deploy + trigger DAG (3.3)"
    & "$SRC\03_big_data\deploy_dag.ps1"
    # Trigger through the scheduler container (deploy_dag.ps1 already waited for it to parse the DAG)
    # and retry, because the run is only accepted once the DAG is registered.
    $triggered = $false
    for ($i = 0; $i -lt 12 -and -not $triggered; $i++) {
        docker exec presight-airflow-scheduler airflow dags trigger presight_etl_pipeline 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) { $triggered = $true } else { Start-Sleep -Seconds 10 }
    }
    if (-not $triggered) { throw "Could not trigger presight_etl_pipeline" }
    Write-Host "DAG triggered -- progress at http://localhost:8081 (admin / admin)." -ForegroundColor DarkGray
} else { Write-Host "Skipping Step 7 (Airflow)." -ForegroundColor DarkGray }

# ---------------------------------------------------------------------------
# Pillar 4 -- Infrastructure & Governance
# ---------------------------------------------------------------------------
Step 8 "Data quality framework (4.3) -> $OUT\04_infrastructure"
Run $PY "$SRC\04_infrastructure\dq_framework.py"

if (-not $SkipDocker) {
    Step 9 "Docker: containerise the ETL pipeline (4.1)"
    Run docker build -t presight-etl .
    Run docker run --rm -v "${PWD}/$OUT/04_infrastructure:/app/outputs" presight-etl
} else { Write-Host "Skipping Step 9 (Docker)." -ForegroundColor DarkGray }

Write-Host ""
Write-Host "4.2 Data Governance is a static document: $SRC\04_infrastructure\data_governance.md" -ForegroundColor DarkGray
Write-Host ""
Write-Host ("All done in {0:N0} s. Results in $OUT" -f ((Get-Date) - $started).TotalSeconds) -ForegroundColor Green
