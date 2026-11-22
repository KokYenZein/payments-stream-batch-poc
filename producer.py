"""publish synthetic payment events to kafka.

a small share of events is deliberately messy, because the point of the poc
is to show how the two paths cope with it:
  - duplicates: the same event sent twice (a retry after a timeout)
  - late events: event_time is one or two days in the past
  - bad events: fail validation and should end up in the dead letter folder
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

import config


def ensure_topic():
    admin = KafkaAdminClient(bootstrap_servers=config.BOOTSTRAP_SERVERS)
    try:
        admin.create_topics(
            [NewTopic(config.TOPIC, num_partitions=config.PARTITIONS, replication_factor=1)]
        )
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def make_event(rng, now):
    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
        "account_id": f"ACC{rng.randint(1, 200):04d}",
        "merchant_category": rng.choice(config.CATEGORIES),
        "amount": round(rng.uniform(1, 500), 2),
        "currency": config.CURRENCY,
        "event_time": now.isoformat(),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=2000)
    parser.add_argument("--rate", type=float, default=400, help="events per second")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    ensure_topic()
    producer = KafkaProducer(
        bootstrap_servers=config.BOOTSTRAP_SERVERS,
        acks="all",  # wait for the broker to confirm every write
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
    )

    counts = {"valid": 0, "duplicate": 0, "late": 0, "bad": 0}
    for _ in range(args.events):
        event = make_event(rng, datetime.now(timezone.utc))
        # keying by account keeps one account's events in order on one partition
        key = event["account_id"]
        roll = rng.random()

        if roll < 0.01:
            # bad event: negative amount or missing account
            if rng.random() < 0.5:
                event["amount"] = -event["amount"]
            else:
                event["account_id"] = None
            counts["bad"] += 1
            producer.send(config.TOPIC, key=key, value=event)
        else:
            if roll < 0.03:
                late_by = timedelta(days=rng.randint(1, 2))
                event["event_time"] = (datetime.now(timezone.utc) - late_by).isoformat()
                counts["late"] += 1
            counts["valid"] += 1
            producer.send(config.TOPIC, key=key, value=event)
            if rng.random() < 0.03:
                counts["duplicate"] += 1
                producer.send(config.TOPIC, key=key, value=event)

        time.sleep(1 / args.rate)

    producer.flush()
    producer.close()

    sent = counts["valid"] + counts["duplicate"] + counts["bad"]
    print(f"sent {sent} messages to topic '{config.TOPIC}'")
    print(f"  unique valid events : {counts['valid']} (of which late: {counts['late']})")
    print(f"  duplicate sends     : {counts['duplicate']}")
    print(f"  bad events          : {counts['bad']}")


if __name__ == "__main__":
    main()
