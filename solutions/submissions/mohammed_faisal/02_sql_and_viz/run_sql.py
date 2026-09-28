"""
Task 2.1 / 2.3 driver — builds the warehouse and runs the SQL files, printing every SELECT result.

  python solutions/submissions/mohammed_faisal/02_sql_and_viz/run_sql.py           # run everything
  python solutions/submissions/mohammed_faisal/02_sql_and_viz/run_sql.py --bench   # + benchmark Section 4

Order: 01_foundations/data_model.sql -> queries.sql -> query_optimization_duckdb.sql
(query_optimization.sql is the PostgreSQL version — see run_optimization_postgres.py)
Prerequisites (both required):
  python .../01_foundations/etl_pipeline.py   (projects_clean.csv / employees_clean.csv)
  python .../02_sql_and_viz/etl_full.py       (transactions_clean.csv — fact_transactions/dim_vendor
                                               are loaded from this file, not from raw transactions.json)
Warehouse file: outputs/presight_warehouse.duckdb
"""
import argparse
import os
import statistics
import sys
import time

import duckdb

# Repo root is four folders above this file; SQL files use repo-relative paths.
HERE = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(HERE))))
# Execution order: schema + load first, then the business queries, then the optimisation demo.
SQL_FILES = [
    os.path.join(HERE, "..", "01_foundations", "data_model.sql"),
    os.path.join(HERE, "queries.sql"),
    os.path.join(HERE, "query_optimization_duckdb.sql"),
]
OPT_FILE = SQL_FILES[2]
DB_FILE = os.path.join(BASE_DIR, "outputs", "presight_warehouse.duckdb")


def split_sections(sql: str):
    """Return (original_query, rewritten_query) text from query_optimization_duckdb.sql for benchmarking."""
    def block(start_marker, end_marker):
        # Cut the text between two marker comments out of the SQL file.
        a = sql.index(start_marker)
        a = sql.index("\n", a) + 1
        b = sql.index(end_marker, a)
        text = sql[a:b]
        return "\n".join(l for l in text.splitlines() if not l.strip().startswith("-- ---")).strip().rstrip(";")
    return (block("-- ORIGINAL QUERY (unmodified from the starter file)", "-- DuckDB result — original query"),
            block("-- REWRITTEN QUERY", "-- DuckDB result — rewritten query"))


def bench(con, query: str, runs: int = 15):
    """Median wall-clock ms over `runs` executions (first run discarded as warm-up)."""
    # Time each run with a wall clock; the first (cold) run is discarded below.
    times = []
    for _ in range(runs + 1):
        t = time.perf_counter()
        rows = con.execute(query).fetchall()
        times.append((time.perf_counter() - t) * 1000)
    return statistics.median(times[1:]), min(times[1:]), len(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bench", action="store_true")
    ap.add_argument("--db", default=DB_FILE)
    args = ap.parse_args()

    # The SQL reads files with repo-relative paths, so run from the repo root.
    os.chdir(BASE_DIR)                       # SQL uses repo-relative paths
    os.makedirs(os.path.dirname(args.db), exist_ok=True)
    # Rebuild the warehouse from scratch so every run is reproducible.
    if os.path.exists(args.db):
        os.remove(args.db)
    con = duckdb.connect(args.db)

    # Run each SQL file statement by statement and print the result of every SELECT.
    for path in SQL_FILES:
        print(f"\n########## {os.path.basename(path)} ##########")
        sql = open(path, encoding="utf-8").read()
        for i, stmt in enumerate(duckdb.extract_statements(sql), 1):
            first = " ".join(stmt.query.split())[:90]
            rel = con.execute(stmt.query)
            if stmt.type == duckdb.StatementType.SELECT:
                df = rel.fetchdf()
                print(f"\n--- [{i}] {first}\n{df.head(25).to_string(index=False)}\n({len(df)} rows)")

    # Benchmark the original vs rewritten query from query_optimization_duckdb.sql (median of 15 runs).
    if args.bench:
        original, rewritten = split_sections(open(OPT_FILE, encoding="utf-8").read())
        print("\n================ BENCHMARK (DuckDB) ================")
        for name, q in [("original", original), ("rewritten", rewritten)]:
            med, best, n = bench(con, q)
            print(f"{name:10s} median={med:7.2f} ms  best={best:7.2f} ms  rows={n}")
        for name, q in [("original", original), ("rewritten", rewritten)]:
            print(f"\n----- EXPLAIN ANALYZE ({name}) -----")
            plan = con.execute("EXPLAIN ANALYZE " + q).fetchall()
            print((plan[0][1] if len(plan[0]) > 1 else str(plan)).encode("ascii", "replace").decode())
    con.close()


if __name__ == "__main__":
    sys.exit(main())
