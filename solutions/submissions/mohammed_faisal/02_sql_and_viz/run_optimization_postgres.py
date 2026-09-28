"""
Task 2.3 — runs query_optimization.sql (the PostgreSQL version) against a real PostgreSQL 15
instance, so the EXPLAIN ANALYZE plans and timings documented in that file's comments can be
reproduced rather than taken on faith. (query_optimization_duckdb.sql is the DuckDB version, run
by run_sql.py.)

Spins up a disposable postgres:15 Docker container (separate from docker-compose.yml's
`presight-postgres`, which serves Airflow's metadata DB and is never touched by this script),
stages the raw files into it, then parses the setup block, original query, rewritten query and
indexes straight out of query_optimization.sql (split on its section headings — no SQL is
duplicated here), benchmarks original vs rewritten before and after the indexes, and prints
EXPLAIN ANALYZE + median timings.

  python solutions/submissions/mohammed_faisal/02_sql_and_viz/run_optimization_postgres.py
  python .../run_optimization_postgres.py --keep     # leave the container running afterwards
  python .../run_optimization_postgres.py --port 5433 --runs 15

Requires: Docker Desktop running (everything goes through `docker exec psql`).
"""
import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(HERE))))
DATA_DIR = os.path.join(REPO_ROOT, "datasets")
SQL_FILE = os.path.join(HERE, "query_optimization.sql")
CONTAINER = "presight-pg-bench"
IMAGE = "postgres:15"
DB_USER, DB_PASS, DB_NAME = "presight", "presight123", "presight_bench"


def sh(cmd, **kw):
    """Run a shell command, streaming nothing, raising on failure unless check=False."""
    return subprocess.run(cmd, shell=True, text=True, capture_output=True, **kw)


def log(msg):
    print(f"[pg-bench] {msg}", flush=True)


# ── Split query_optimization.sql on its section headings ──────────────────────────────────────
# Single source of truth: nothing here is a hand-copied duplicate of that file's SQL text.

MARK_ORIGINAL = "-- ORIGINAL QUERY (unmodified from the starter file)"
MARK_ORIGINAL_PLAN = "-- EXPLAIN ANALYZE output — original query"
MARK_REWRITTEN = "-- REWRITTEN QUERY — before indexes"
MARK_REWRITTEN_PLAN = "-- EXPLAIN ANALYZE output — rewritten query, before indexes"
MARK_INDEXES = "-- INDEXES —"
MARK_AFTER = "-- REWRITTEN QUERY — after indexes"


def sql_only(block):
    """Drop comment lines and psql backslash meta-commands such as \\timing, keep the SQL."""
    return "\n".join(l for l in block.splitlines()
                     if l.strip() and not l.strip().startswith("--") and not l.strip().startswith("\\"))


def bare_query(block):
    """Strip comments and the leading EXPLAIN ANALYZE so the block runs as a plain query."""
    text = sql_only(block).strip()
    if text.upper().startswith("EXPLAIN ANALYZE"):
        text = text[len("EXPLAIN ANALYZE"):]
    return text.strip().rstrip(";")


def load_sections():
    sql = open(SQL_FILE, encoding="utf-8").read()
    setup = sql_only(sql[:sql.index(MARK_ORIGINAL)])
    original = bare_query(sql[sql.index(MARK_ORIGINAL):sql.index(MARK_ORIGINAL_PLAN)])
    rewritten = bare_query(sql[sql.index(MARK_REWRITTEN):sql.index(MARK_REWRITTEN_PLAN)])
    indexes = sql_only(sql[sql.index(MARK_INDEXES):sql.index(MARK_AFTER)])
    return setup, original, rewritten, indexes


# ── Docker container lifecycle ───────────────────────────────────────────────────────────────

def ensure_container(port):
    running = sh(f"docker inspect -f {{{{.State.Running}}}} {CONTAINER}", check=False)
    if running.returncode == 0 and running.stdout.strip() == "true":
        log(f"Reusing already-running container {CONTAINER}")
        return
    sh(f"docker rm -f {CONTAINER}", check=False)  # clean up a stopped/stale one, if any
    log(f"Starting {IMAGE} as {CONTAINER} on host port {port} (isolated from docker-compose's Postgres)")
    r = sh(f'docker run -d --name {CONTAINER} -p {port}:5432 '
           f'-e POSTGRES_USER={DB_USER} -e POSTGRES_PASSWORD={DB_PASS} -e POSTGRES_DB={DB_NAME} {IMAGE}')
    if r.returncode != 0:
        sys.exit(f"docker run failed:\n{r.stderr}")
    for _ in range(30):
        ready = sh(f"docker exec {CONTAINER} pg_isready -U {DB_USER}", check=False)
        if ready.returncode == 0:
            log("Postgres is ready")
            return
        time.sleep(2)
    sys.exit("Postgres never became ready")


def psql(sql_text=None, sql_file=None):
    """Run SQL inside the container via psql; returns the completed process."""
    if sql_file:
        dest = f"/tmp/{os.path.basename(sql_file)}"
        sh(f'docker cp "{sql_file}" {CONTAINER}:{dest}')
        return sh(f"docker exec {CONTAINER} psql -U {DB_USER} -d {DB_NAME} -f {dest}")
    tmp = os.path.join(REPO_ROOT, "outputs", "_pg_bench_tmp.sql")
    os.makedirs(os.path.dirname(tmp), exist_ok=True)
    open(tmp, "w", encoding="utf-8").write(sql_text)
    r = psql(sql_file=tmp)
    os.remove(tmp)
    return r


# ── Stage the raw files where query_optimization.sql's server-side COPY expects them ───────

def stage_files():
    # COPY needs CSV; transactions ships as JSON, so flatten it to a temp CSV first.
    import csv
    records = json.load(open(os.path.join(DATA_DIR, "transactions.json"), encoding="utf-8"))
    tmp_csv = os.path.join(REPO_ROOT, "outputs", "_transactions_flat.csv")
    os.makedirs(os.path.dirname(tmp_csv), exist_ok=True)
    cols = ["transaction_id", "project_id", "vendor_id", "vendor_name", "category", "amount",
            "currency", "transaction_date", "approved_by", "payment_status", "invoice_ref", "notes"]
    with open(tmp_csv, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        for r in records:
            w.writerow({c: r.get(c) for c in cols})

    for name, path in [("employees", os.path.join(DATA_DIR, "employees.csv")),
                       ("projects", os.path.join(DATA_DIR, "projects.csv")),
                       ("transactions", tmp_csv)]:
        r = sh(f'docker cp "{path}" {CONTAINER}:/tmp/{name}.csv')
        if r.returncode != 0:
            sys.exit(f"Staging {name} failed:\n{r.stderr}")
    os.remove(tmp_csv)
    log("Staged employees / projects / transactions into the container's /tmp")


def run_setup(setup_sql):
    r = psql(sql_text=setup_sql)
    if r.returncode != 0 or "ERROR" in r.stderr:
        sys.exit(f"Setup block failed:\n{r.stderr}")
    counts = psql(sql_text="SELECT (SELECT COUNT(*) FROM employees), (SELECT COUNT(*) FROM projects), "
                           "(SELECT COUNT(*) FROM transactions);").stdout.splitlines()
    log(f"Loaded via the file's setup block (employees | projects | transactions): {counts[2].strip()}")


# ── Benchmark ─────────────────────────────────────────────────────────────────────────────────

def explain_analyze(query):
    r = psql(sql_text=f"EXPLAIN (ANALYZE, BUFFERS) {query};")
    if r.returncode != 0:
        sys.exit(f"EXPLAIN failed:\n{r.stderr}")
    return r.stdout


def median_exec_time_ms(query, runs):
    tmp = os.path.join(REPO_ROOT, "outputs", "_pg_bench_reps.sql")
    body = "".join(f"EXPLAIN (ANALYZE, TIMING OFF, SUMMARY ON) {query};\n" for _ in range(runs))
    open(tmp, "w", encoding="utf-8").write(body)
    r = psql(sql_file=tmp)
    os.remove(tmp)
    times = [float(m) for m in re.findall(r"Execution Time: ([\d.]+) ms", r.stdout)]
    return statistics.median(times), times


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=5433, help="host port for the disposable container")
    ap.add_argument("--runs", type=int, default=15, help="timed runs per variant (median reported)")
    ap.add_argument("--keep", action="store_true", help="leave the container running afterwards")
    args = ap.parse_args()

    setup, original, rewritten, indexes = load_sections()
    log(f"Parsed setup, ORIGINAL ({len(original)} chars), REWRITTEN ({len(rewritten)} chars) and the "
        f"INDEXES block from {os.path.basename(SQL_FILE)}")

    ensure_container(args.port)
    stage_files()
    run_setup(setup)

    print("\n" + "=" * 70)
    print("BEFORE INDEXES (primary keys only)")
    print("=" * 70)
    results = {}
    for name, q in [("original", original), ("rewritten", rewritten)]:
        med, _ = median_exec_time_ms(q, args.runs)
        results[name] = med
        print(f"  {name:10s} median exec time: {med:7.2f} ms  (n={args.runs})")
    for name, q in [("original", original), ("rewritten", rewritten)]:
        print(f"\n----- EXPLAIN ANALYZE: {name} (no indexes) -----")
        print(explain_analyze(q))

    log("Creating the indexes from the INDEXES block")
    r = psql(sql_text=indexes)
    if r.returncode != 0 or "ERROR" in r.stderr:
        sys.exit(f"Index creation failed:\n{r.stderr}")

    print("\n" + "=" * 70)
    print("AFTER INDEXES")
    print("=" * 70)
    for name, q in [("original", original), ("rewritten", rewritten)]:
        med, _ = median_exec_time_ms(q, args.runs)
        results[name + "_idx"] = med
        print(f"  {name:10s} median exec time: {med:7.2f} ms  (n={args.runs})")
    print("\n----- EXPLAIN ANALYZE: rewritten (with indexes) -----")
    print(explain_analyze(rewritten))

    print("\n" + "=" * 70)
    print(f"SUMMARY: original {results['original']:.2f} ms -> rewritten + indexes "
          f"{results['rewritten_idx']:.2f} ms = {results['original'] / results['rewritten_idx']:.1f}x")
    print("=" * 70)

    if args.keep:
        log(f"Container {CONTAINER} left running on port {args.port} "
           f"(psql -h localhost -p {args.port} -U {DB_USER} -d {DB_NAME}, password: {DB_PASS})")
    else:
        sh(f"docker rm -f {CONTAINER}")
        log(f"Removed container {CONTAINER} (pass --keep to leave it running)")


if __name__ == "__main__":
    main()
