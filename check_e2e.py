"""end-to-end check, run after the producer, the consumer and the batch job.

the producer is seeded, so the same events are generated on every run and the
expected numbers below are fixed. with --after-replay it also checks that
re-reading the whole topic inflated the live totals but left the batch
tables unchanged.
"""
import argparse
import json
import sys

import duckdb

import config

EXPECTED_UNIQUE_VALID = 1980
EXPECTED_DUPLICATES = 51
EXPECTED_BAD = 20
EXPECTED_LATE = 41


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--after-replay", action="store_true")
    args = parser.parse_args()

    con = duckdb.connect()
    bronze_ids, bronze_unique = con.execute(
        f"select count(*), count(distinct event_id) from read_json_auto('{config.BRONZE_DIR}/*.jsonl')"
    ).fetchone()
    silver_rows, silver_unique, late = con.execute(
        f"select count(*), count(distinct event_id), count(*) filter (where is_late) from '{config.SILVER_FILE}'"
    ).fetchone()
    silver_amount = con.execute(f"select sum(amount) from '{config.SILVER_FILE}'").fetchone()[0]
    gold_events, gold_amount = con.execute(
        f"select sum(events), sum(amount) from '{config.GOLD_FILE}'"
    ).fetchone()
    dead_letters = sum(
        len(p.read_text().splitlines()) for p in config.DEAD_LETTER_DIR.glob("*.jsonl")
    )
    live = json.loads(config.LIVE_TOTALS_FILE.read_text())
    live_events = sum(v["events"] for v in live.values())

    checks = {
        "silver has one row per valid event": silver_rows == EXPECTED_UNIQUE_VALID,
        "silver has no duplicate event ids": silver_rows == silver_unique,
        "bronze still holds every unique event": bronze_unique == EXPECTED_UNIQUE_VALID,
        "late events were re-dated": late >= EXPECTED_LATE,
        "gold adds up to silver": gold_events == silver_rows and gold_amount == silver_amount,
    }
    if args.after_replay:
        checks["replay inflated the live totals"] = live_events >= 2 * EXPECTED_UNIQUE_VALID
    else:
        checks["bronze kept the duplicate sends"] = bronze_ids == EXPECTED_UNIQUE_VALID + EXPECTED_DUPLICATES
        checks["every bad event reached the dead letter folder"] = dead_letters == EXPECTED_BAD
        checks["live totals counted the duplicates"] = live_events == EXPECTED_UNIQUE_VALID + EXPECTED_DUPLICATES

    print(f"bronze rows {bronze_ids}, silver rows {silver_rows}, late {late}, "
          f"dead letters {dead_letters}, live events {live_events}")
    failed = False
    for name, ok in checks.items():
        failed = failed or not ok
        print(f"{'PASS' if ok else 'FAIL'}  {name}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
