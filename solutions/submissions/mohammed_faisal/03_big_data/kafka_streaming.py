"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
File: kafka_streaming.py  (solution)
Author: Mohammed Faisal
Pillar: Big Data Processing — Task 3.2
=============================================================

ARCHITECTURE
------------
  JSONL file -> Producer -> [presight.project.events] -> Consumer -> outputs/kafka/summary.json
                                                            |
                                        Critical escalations -> [presight.escalations.critical]

HOW TO RUN
----------
  docker compose up -d zookeeper kafka        # Kafka on localhost:9092
  pip install kafka-python

  python solutions/submissions/mohammed_faisal/03_big_data/kafka_streaming.py --mode producer      # 8,333 msgs @ 50 ms  (~7 min)
  python solutions/submissions/mohammed_faisal/03_big_data/kafka_streaming.py --mode consumer
  python solutions/submissions/mohammed_faisal/03_big_data/kafka_streaming.py --mode both          # produce then consume

Options: --delay 0.05  seconds between messages (0 = as fast as possible)
         --limit N     only send the first N events (quick tests)
Env:     KAFKA_BOOTSTRAP (default localhost:9092)
"""

import argparse
import json
import logging
import os
import time
from collections import Counter
from datetime import datetime, timezone

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
EVENTS_FILE = os.path.join(BASE_DIR, "datasets", "events_stream", "events_2025_01.jsonl")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs", "results", "mohammed_faisal", "03_big_data", "kafka")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Kafka config ───────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_EVENTS = "presight.project.events"
TOPIC_ESCALATIONS = "presight.escalations.critical"
CONSUMER_GROUP = "presight-assessment-consumer"
LOG_EVERY = 100
CONSUMER_IDLE_TIMEOUT_MS = 10000


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ==============================================================================
# 3.2a SETUP — Create Kafka topics
# ==============================================================================

def create_topics(retries: int = 10):
    """Create both topics if missing (idempotent). Waits for the broker to come up."""
    admin = None
    for attempt in range(1, retries + 1):
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, client_id="presight-admin")
            break
        except KafkaError:   # NoBrokersAvailable in kafka-python 2.x
            logger.warning("Kafka not reachable at %s (attempt %d/%d)", KAFKA_BOOTSTRAP, attempt, retries)
            time.sleep(3)
    if admin is None:
        raise SystemExit(f"Could not connect to Kafka at {KAFKA_BOOTSTRAP}. Is `docker compose up -d` running?")

    # Topic layout required by Task 3.2a: 3 partitions for events, 1 for critical escalations.
    wanted = [
        NewTopic(name=TOPIC_EVENTS, num_partitions=3, replication_factor=1),
        NewTopic(name=TOPIC_ESCALATIONS, num_partitions=1, replication_factor=1),
    ]
    for topic in wanted:
        try:
            admin.create_topics([topic])
            logger.info("Created topic %s (%d partitions)", topic.name, topic.num_partitions)
        # Idempotent: running the script again is fine.
        except TopicAlreadyExistsError:
            logger.info("Topic %s already exists", topic.name)
    admin.close()


# ==============================================================================
# 3.2b PRODUCER
# ==============================================================================

# Producer: JSON values, string keys, acks=all so a message is confirmed by the broker.
def build_producer() -> KafkaProducer:
    """Producer with JSON values and string keys."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        request_timeout_ms=30000,
        acks="all",
        retries=3,
    )


def run_producer(producer, events_file: str, delay_seconds: float = 0.05, limit: int = None) -> int:
    """
    Stream events from the JSONL file. Key = event_type, so all events of one type land in the same
    partition and keep their relative order. `produced_at` is added to each message.
    """
    logger.info("Starting producer — streaming events from: %s", events_file)
    sent = 0
    with open(events_file, "r", encoding="utf-8") as fh:
        # The source file is JSONL: one JSON event per line.
        for line in fh:
            if not line.strip():
                continue
            event = json.loads(line)
            # Add the produced_at timestamp required by Task 3.2b.
            event["produced_at"] = utc_now_iso()
            # The key is event_type, so events of one type stay in one partition (ordering).
            producer.send(TOPIC_EVENTS, key=event.get("event_type"), value=event)
            sent += 1
            if sent % LOG_EVERY == 0:
                logger.info("Produced %d messages (last: %s / %s)", sent, event["event_id"], event["event_type"])
            if limit and sent >= limit:
                break
            if delay_seconds:
                # Simulated stream rate (50 ms per message by default).
                time.sleep(delay_seconds)          # simulate stream rate (50 ms default)
    # Block until every buffered message has been delivered to the broker.
    producer.flush()
    producer.close()
    logger.info("Producer finished — total sent: %d", sent)
    return sent


# ==============================================================================
# 3.2c CONSUMER
# ==============================================================================

# Consumer: reads from the beginning and stops after 10 s without messages.
def build_consumer(topic: str) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=CONSUMER_IDLE_TIMEOUT_MS,      # stop after 10 s without messages
    )


def run_consumer(consumer) -> dict:
    """
    Consume the topic, count per event_type, forward Critical escalations (3.2d) to
    presight.escalations.critical and write outputs/kafka/summary.json (3.2e).
    """
    logger.info("Starting consumer — listening on: %s", TOPIC_EVENTS)

    counts = Counter()
    total = 0
    forwarded = 0
    # Second producer used only for the forwarding topic.
    forward_producer = build_producer()
    started = time.time()

    for message in consumer:
        event = message.value
        total += 1
        # Running count per event_type for the summary.
        counts[event.get("event_type", "unknown")] += 1

        # 3.2d escalation forwarding
        payload = event.get("payload") or {}
        # Task 3.2d: forward Critical escalations to their own topic.
        if event.get("event_type") == "escalation_raised" and payload.get("severity") == "Critical":
            forward_producer.send(TOPIC_ESCALATIONS, key=event.get("event_type"), value=event)
            forwarded += 1

        if total % LOG_EVERY == 0:
            logger.info("Consumed %d messages (last: %s / %s / %s) | critical forwarded: %d",
                        total, event.get("event_id"), event.get("event_type"), event.get("project_id"), forwarded)

    forward_producer.flush()
    forward_producer.close()
    # Throughput is measured up to the last message, excluding the 10 s idle wait for the timeout.
    # Throughput is measured up to the last message, excluding the idle timeout wait.
    active_seconds = max(time.time() - started - CONSUMER_IDLE_TIMEOUT_MS / 1000, 1e-9)
    logger.info("Consumer finished — consumed=%d, critical escalations forwarded=%d", total, forwarded)

    # Task 3.2e: the summary that is written to outputs/kafka/summary.json.
    summary = {
        "run_at": utc_now_iso(),
        "topic": TOPIC_EVENTS,
        "total_messages_consumed": total,
        "event_counts": dict(sorted(counts.items())),
        "critical_escalations_forwarded": forwarded,
        "forwarded_to_topic": TOPIC_ESCALATIONS,
        "throughput_messages_per_second": round(total / active_seconds, 2) if total else 0.0,
        "elapsed_seconds": round(active_seconds, 2),
    }
    summary_path = os.path.join(OUTPUT_DIR, "summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    logger.info("Summary written to: %s", summary_path)
    return summary


# ==============================================================================
# ENTRY POINT
# ==============================================================================

def main():
    # CLI: run the producer, the consumer, or both one after the other.
    parser = argparse.ArgumentParser(description="Kafka assessment — producer/consumer")
    parser.add_argument("--mode", choices=["producer", "consumer", "both"], default="both",
                        help="Run as producer, consumer, or both")
    parser.add_argument("--delay", type=float, default=0.05, help="seconds between produced messages")
    parser.add_argument("--limit", type=int, default=None, help="only send the first N events")
    args = parser.parse_args()

    create_topics()

    if args.mode in ("producer", "both"):
        run_producer(build_producer(), EVENTS_FILE, args.delay, args.limit)

    if args.mode in ("consumer", "both"):
        summary = run_consumer(build_consumer(TOPIC_EVENTS))
        print("\nEvent summary:", json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
