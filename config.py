"""settings shared by the producer, the stream consumer and the batch job.

anything that differs between machines is read from the environment, with a
default that works for the local docker setup.
"""
import os
from pathlib import Path

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "payments")
PARTITIONS = 3
# a new group name makes the consumer re-read the topic from the start
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "payments-stream-consumer")

DATA_DIR = Path(__file__).parent / "data"
BRONZE_DIR = DATA_DIR / "bronze"            # valid events, exactly as they arrived
DEAD_LETTER_DIR = DATA_DIR / "dead_letter"  # rejected events with a reason
SILVER_FILE = DATA_DIR / "silver" / "payments.parquet"
GOLD_FILE = DATA_DIR / "gold" / "daily_category_totals.parquet"
LIVE_TOTALS_FILE = DATA_DIR / "live_totals.json"

CATEGORIES = ["groceries", "transport", "dining", "bills", "shopping"]
CURRENCY = "MYR"
