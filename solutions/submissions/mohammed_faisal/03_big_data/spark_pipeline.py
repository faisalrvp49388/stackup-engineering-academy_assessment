"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
File: spark_pipeline.py  (solution)
Author: Mohammed Faisal
Pillar: Big Data Processing — Task 3.1
=============================================================

Processes ALL monthly JSONL event files (datasets/events_stream/events_*.jsonl)
and writes five Parquet tables to outputs/spark/:

  1. project_activity_summary
  2. user_activity_summary
  3. escalation_log            (partitioned by severity)
  4. daily_event_volume        (partitioned by event_date)
  5. peak_usage_analysis

HOW TO RUN
----------
  python solutions/submissions/mohammed_faisal/03_big_data/spark_pipeline.py
  (Windows without Hadoop libs: use Dockerfile.spark in this folder)

Requires: pyspark and Java 11/17.  Override paths with EVENTS_DIR / SPARK_OUTPUT_DIR.
"""

import os
import sys
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, MapType
)

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
EVENTS_DIR = os.environ.get("EVENTS_DIR", os.path.join(BASE_DIR, "datasets", "events_stream"))
OUTPUT_DIR = os.environ.get("SPARK_OUTPUT_DIR", os.path.join(BASE_DIR, "outputs", "results", "mohammed_faisal", "03_big_data", "spark"))

# Per-stage timings collected for the performance baseline (3.1e)
# Stage timings, printed as the performance baseline at the end.
TIMINGS = {}


def log(msg: str):
    print(f"[spark_pipeline] {msg}", flush=True)


# ==============================================================================
# STEP 1 — Initialise Spark
# ==============================================================================

# Session factory (local mode, UTC time zone, small shuffle partition count).
def get_spark_session() -> SparkSession:
    """Local SparkSession using all cores, tuned for a ~20 MB dataset."""
    # Make sure the Python workers use the same interpreter as the driver.
    # Workers must use the same Python interpreter as the driver.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (
        SparkSession.builder
        .appName("PresightEventsProcessing")
        # Local mode using every available core.
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")     # timestamps are UTC ('Z')
        .config("spark.sql.shuffle.partitions", "8")      # default 200 is wasteful for 100K rows
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ==============================================================================
# STEP 2 — Load events (3.1a)
# ==============================================================================

# Explicit schema (no inferSchema on 100K rows); payload stays a flexible string map.
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("project_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("payload", MapType(StringType(), StringType()), True),  # flexible nested JSON
])


def load_events(spark: SparkSession, events_dir: str):
    """Load every monthly file with a wildcard and an explicit schema (no inferSchema)."""
    path = os.path.join(events_dir, "events_*.jsonl")
    df = (
        spark.read
        # Explicit schema + PERMISSIVE mode: bad lines become nulls instead of failing the job.
        .schema(EVENT_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ssX")   # e.g. 2025-01-11T12:08:34Z
        .json(path)
    )
    count = df.count()
    log(f"Loaded {count:,} raw events from {path}")
    return df


# ==============================================================================
# STEP 3 — Validate and clean (3.1b)
# ==============================================================================

def validate_events(df):
    """Drop null keys, de-duplicate event_id (earliest timestamp wins), add date parts."""
    # Row counts before and after each cleaning step (Task 3.1b logging).
    n0 = df.count()

    # Drop events that have no event id or no user.
    df = df.dropna(subset=["event_id", "user_id"])
    n1 = df.count()
    log(f"Drop null event_id/user_id : {n0:,} -> {n1:,}  (dropped {n0 - n1:,})")

    # Keep the first occurrence of each event_id by timestamp
    # Keep the earliest occurrence of each duplicated event_id.
    w = Window.partitionBy("event_id").orderBy(F.col("timestamp").asc_nulls_last())
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    n2 = df.count()
    log(f"Remove duplicate event_id  : {n1:,} -> {n2:,}  (dropped {n1 - n2:,})")

    df = (
        # Derived date parts used for grouping and partitioning.
        df.withColumn("event_date", F.to_date("timestamp"))
          .withColumn("event_hour", F.hour("timestamp"))
          .withColumn("event_month", F.date_format("timestamp", "yyyy-MM"))
    )
    # Cached: reused by five aggregations
    # Cached because the five aggregations all read this DataFrame.
    df = df.cache()
    log(f"Clean events ready         : {df.count():,} rows")
    return df


# ==============================================================================
# STEP 4 — Aggregations (3.1c)
# ==============================================================================

def project_activity_summary(df):
    """Table 1 — one row per project (login events, which have no project, excluded)."""
    return (
        df.filter(F.col("project_id").isNotNull())
        .groupBy("project_id")
        .agg(
            F.count("*").alias("total_events"),
            F.sum((F.col("event_type") == "escalation_raised").cast("int")).alias("escalation_count"),
            F.sum((F.col("event_type") == "task_completed").cast("int")).alias("task_completions"),
            F.sum((F.col("event_type") == "document_uploaded").cast("int")).alias("document_uploads"),
            F.max("timestamp").alias("last_event_timestamp"),
            F.countDistinct("user_id").alias("unique_users"),
            F.countDistinct("event_type").alias("unique_event_types"),
        )
        .orderBy(F.col("total_events").desc())
    )


def user_activity_summary(df):
    """Table 2 — one row per user."""
    # Logins/logouts are session events, not user 'actions'.
    is_session = F.col("event_type").isin("login", "logout")
    return (
        df.groupBy("user_id")
        .agg(
            F.sum((F.col("event_type") == "login").cast("int")).alias("login_count"),
            F.sum((F.col("event_type") == "logout").cast("int")).alias("logout_count"),
            F.sum((~is_session).cast("int")).alias("actions_taken"),
            F.countDistinct("project_id").alias("projects_touched"),   # countDistinct ignores nulls
            F.min("timestamp").alias("first_active"),
            F.max("timestamp").alias("last_active"),
            F.countDistinct("event_date").alias("active_days"),
        )
        .orderBy(F.col("actions_taken").desc())
    )


def escalation_log(df):
    """
    Table 3 — every raised escalation with its matching resolution (if any).

    Matching rule: a resolution belongs to the raised event of the same project whose
    raise time is the latest one <= resolved_at and (if the project was escalated again
    later) that resolution happened before the next raise. Because a project has only one
    open escalation at a time, this yields at most one resolution per raise and never
    re-uses one resolution for two raises. Left join keeps unresolved escalations.
    """
    # Order raises per project so each raise can be matched to the next one.
    w_proj = Window.partitionBy("project_id").orderBy("raised_at")
    raised = (
        df.filter(F.col("event_type") == "escalation_raised")
        .select(
            "event_id", "project_id",
            F.col("user_id").alias("raised_by"),
            F.col("timestamp").alias("raised_at"),
            F.col("payload")["severity"].alias("severity"),
        )
        .withColumn("next_raised_at", F.lead("raised_at").over(w_proj))
    )
    resolved = (
        df.filter(F.col("event_type") == "escalation_resolved")
        .select(
            F.col("project_id").alias("r_project_id"),
            F.col("payload")["resolved_by"].alias("resolved_by"),
            F.col("timestamp").alias("resolved_at"),
        )
    )
    # A resolution matches a raise on the same project, at/after it and before the next raise.
    cond = (
        (raised.project_id == resolved.r_project_id)
        & (resolved.resolved_at >= raised.raised_at)
        & (raised.next_raised_at.isNull() | (resolved.resolved_at < raised.next_raised_at))
    )
    joined = raised.join(resolved, cond, "left")

    # Earliest resolution per raised event
    # Keep only the earliest matching resolution per raised event.
    w_pick = Window.partitionBy("event_id").orderBy(F.col("resolved_at").asc_nulls_last())
    joined = joined.withColumn("_rn", F.row_number().over(w_pick)).filter("_rn = 1")

    return joined.select(
        "event_id", "project_id", "raised_by", "raised_at",
        F.coalesce("severity", F.lit("Unknown")).alias("severity"),
        F.col("resolved_at").isNotNull().alias("resolved"),
        "resolved_by", "resolved_at",
        F.round((F.unix_timestamp("resolved_at") - F.unix_timestamp("raised_at")) / 3600, 2)
         .alias("resolution_time_hours"),
    ).orderBy("raised_at")


def daily_event_volume(df):
    """Table 4 — counts per day & type with a running total per event type."""
    daily = df.groupBy("event_date", "event_type").agg(F.count("*").alias("event_count"))
    # Running total per event type over time.
    w = (Window.partitionBy("event_type").orderBy("event_date")
         .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    return (
        daily.withColumn("cumulative_count", F.sum("event_count").over(w))
        .orderBy(F.col("event_date").asc(), F.col("event_count").desc())
    )


def peak_usage_analysis(df):
    """Table 5 — the 20 busiest date x hour windows."""
    return (
        df.groupBy("event_date", "event_hour")
        .agg(
            F.count("*").alias("total_events"),
            F.countDistinct("user_id").alias("unique_users"),
            F.countDistinct("event_type").alias("event_types_per_hour"),
        )
        .orderBy(F.col("total_events").desc(), F.col("event_date").asc(), F.col("event_hour").asc())
        .limit(20)
    )


# ==============================================================================
# STEP 5 — Write outputs (3.1d)
# ==============================================================================

# Write one table as Parquet; partitioned tables get one file per partition value.
def write_parquet(spark, df, name: str, output_dir: str, partition_by: str = None):
    """Write one table (overwrite) with few part files; return rows written."""
    path = os.path.join(output_dir, name)
    if partition_by:
        # One task per partition value -> exactly one file per partition directory.
        # (coalesce(1) would funnel 365 date partitions through a single writer task.)
        writer = df.repartition(partition_by).write.mode("overwrite").partitionBy(partition_by)
    else:
        writer = df.coalesce(1).write.mode("overwrite")  # small table -> one part file
    writer.parquet(path)
    rows = spark.read.parquet(path).count()
    log(f"Wrote {name:<26} -> {path}  ({rows:,} rows"
        f"{', partitioned by ' + partition_by if partition_by else ''})")
    return rows


def timed(label, fn):
    t = time.time()
    result = fn()
    TIMINGS[label] = time.time() - t
    return result


# ==============================================================================
# PIPELINE ENTRY POINT
# ==============================================================================

def run_pipeline():
    t_start = time.time()
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    spark = get_spark_session()

    raw = timed("load", lambda: load_events(spark, EVENTS_DIR))
    total_raw = raw.count()
    clean = timed("validate_and_clean", lambda: validate_events(raw))

    # (table name, builder function, partition column) drives the write loop below.
    tables = [
        ("project_activity_summary", project_activity_summary, None),
        ("user_activity_summary", user_activity_summary, None),
        ("escalation_log", escalation_log, "severity"),
        ("daily_event_volume", daily_event_volume, "event_date"),
        ("peak_usage_analysis", peak_usage_analysis, None),
    ]
    # Run and time each aggregation together with its write (Spark is lazy).
    row_counts = {}
    for name, builder, part in tables:
        row_counts[name] = timed(
            name, lambda b=builder, n=name, p=part: write_parquet(spark, b(clean), n, OUTPUT_DIR, p))

    total_time = time.time() - t_start

    # ── 3.1e Performance baseline ────────────────────────────────────────────────
    # Task 3.1e: performance baseline (per-stage timing, total time, events per second).
    print("\n" + "=" * 62)
    print("PERFORMANCE BASELINE")
    print("=" * 62)
    for label, secs in TIMINGS.items():
        print(f"  {label:<28} {secs:7.2f} s")
    print("-" * 62)
    print(f"  Total execution time         {total_time:7.2f} s")
    print(f"  Total rows processed         {total_raw:,}")
    print(f"  Events processed per second  {total_raw / total_time:,.0f}")
    print("  Output row counts:", row_counts)
    print("=" * 62)

    spark.stop()
    print("Spark pipeline complete.")


if __name__ == "__main__":
    run_pipeline()
