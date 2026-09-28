"""Launches DuckDB's local web UI against the Presight warehouse, and keeps the process alive."""
import time
import duckdb

DB = "outputs/presight_warehouse.duckdb"

con = duckdb.connect(DB)  # read-write, so the UI can also run ad-hoc writes if you want
con.load_extension("ui")
con.sql("SET ui_local_port = 4214;")  # 4213 is held by a stale, unresponsive process
result = con.sql("CALL start_ui();").fetchall()
print("start_ui() ->", result, flush=True)
print(f"DuckDB UI running against {DB} — leave this process running.", flush=True)

while True:
    time.sleep(3600)
