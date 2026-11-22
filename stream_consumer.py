"""streaming path: read payment events from kafka as they arrive.

for every small batch of messages it does three things, in this order:
  1. validate each event; bad ones go to the dead letter folder with a reason
  2. land the valid ones in the bronze folder and update the live totals
  3. commit the kafka offsets

committing only after the write means a crash can replay messages but never
lose them (at-least-once). the cost is that the live totals can count an
event twice. the batch job is what corrects that.
"""
import argparse
import json
import os
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer

import config


def validate(raw):
    """return (event, None) if the event is usable, else (None, reason)."""
    try:
        event = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return None, "not valid json"
    if not isinstance(event, dict):
        return None, "not a json object"
    for field in ("event_id", "account_id", "merchant_category", "event_time"):
        if not event.get(field):
            return None, f"missing {field}"
    amount = event.get("amount")
    if not isinstance(amount, (int, float)) or isinstance(amount, bool) or amount <= 0:
        return None, "amount must be a positive number"
    if event.get("currency") != config.CURRENCY:
        return None, "unexpected currency"
    return event, None


def write_atomic(path, lines):
    """write to a temp file then rename, so a reader never sees half a file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w") as f:
        f.write("\n".join(lines) + "\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def load_live_totals():
    if config.LIVE_TOTALS_FILE.exists():
        return json.loads(config.LIVE_TOTALS_FILE.read_text())
    return {}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--exit-when-idle",
        type=int,
        default=0,
        help="stop after this many seconds without new events (0 = run until ctrl+c)",
    )
    args = parser.parse_args()

    consumer = KafkaConsumer(
        config.TOPIC,
        bootstrap_servers=config.BOOTSTRAP_SERVERS,
        group_id=config.CONSUMER_GROUP,
        enable_auto_commit=False,      # we commit ourselves, after the write
        auto_offset_reset="earliest",  # a new consumer group starts from the beginning
    )
    live_totals = load_live_totals()
    last_event_at = time.time()
    print(f"listening on topic '{config.TOPIC}' (ctrl+c to stop)")

    try:
        while True:
            polled = consumer.poll(timeout_ms=1000, max_records=500)
            if not polled:
                idle = time.time() - last_event_at
                if args.exit_when_idle and idle > args.exit_when_idle:
                    break
                continue
            last_event_at = time.time()

            landed = rejected = 0
            for tp, records in polled.items():
                # the file name comes from the partition and first offset, so a
                # replayed batch overwrites its own file instead of adding a copy
                name = f"{tp.topic}-p{tp.partition}-o{records[0].offset:012d}.jsonl"
                ingested_at = datetime.now(timezone.utc).isoformat()
                good, bad = [], []
                for record in records:
                    event, reason = validate(record.value)
                    if event is None:
                        bad.append(json.dumps({
                            "reason": reason,
                            "partition": record.partition,
                            "offset": record.offset,
                            "raw": record.value.decode(errors="replace"),
                        }))
                        continue
                    event["_partition"] = record.partition
                    event["_offset"] = record.offset
                    event["_ingested_at"] = ingested_at
                    good.append(json.dumps(event))

                    totals = live_totals.setdefault(
                        event["merchant_category"], {"events": 0, "amount": 0.0}
                    )
                    totals["events"] += 1
                    totals["amount"] = round(totals["amount"] + event["amount"], 2)

                if good:
                    write_atomic(config.BRONZE_DIR / name, good)
                if bad:
                    write_atomic(config.DEAD_LETTER_DIR / name, bad)
                landed += len(good)
                rejected += len(bad)

            write_atomic(config.LIVE_TOTALS_FILE, [json.dumps(live_totals, indent=2)])
            consumer.commit()
            print(f"landed {landed:4d} events, rejected {rejected:2d}, offsets committed")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print("consumer stopped")


if __name__ == "__main__":
    main()
