"""batch path: rebuild clean tables from everything landed in bronze.

this is the job that would run on a schedule (nightly, say). it is slower
than the stream but it sees all the data at once, so it can do what the
stream cannot do cheaply: remove duplicates, place late events on the day
they actually happened, and check the stream's live totals.

it rebuilds its outputs from scratch every run, so running it twice gives
the same result (idempotent).
"""
import json
import sys

import duckdb

import config


def main():
    if not any(config.BRONZE_DIR.glob("*.jsonl")):
        sys.exit("no bronze files yet. run the producer and the stream consumer first.")

    config.SILVER_FILE.parent.mkdir(parents=True, exist_ok=True)
    config.GOLD_FILE.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    con.execute(f"""
        create table bronze as
        select *
        from read_json(
            '{config.BRONZE_DIR}/*.jsonl',
            format = 'newline_delimited',
            columns = {{
                event_id: 'varchar', account_id: 'varchar', merchant_category: 'varchar',
                amount: 'decimal(12,2)', currency: 'varchar', event_time: 'timestamptz',
                _partition: 'integer', _offset: 'bigint', _ingested_at: 'timestamptz'
            }}
        )
    """)

    # silver: one row per event_id. the first arrival wins.
    con.execute("""
        create table silver as
        select
            *,
            cast(event_time at time zone 'UTC' as date) as event_date,
            cast(event_time at time zone 'UTC' as date)
                < cast(_ingested_at at time zone 'UTC' as date) as is_late
        from bronze
        qualify row_number() over (
            partition by event_id order by _ingested_at, _partition, _offset
        ) = 1
    """)

    # gold: daily totals by the day the payment happened, not the day it arrived
    con.execute("""
        create table gold as
        select event_date, merchant_category, count(*) as events, sum(amount) as amount
        from silver
        group by all
        order by all
    """)

    con.execute(f"copy silver to '{config.SILVER_FILE}' (format parquet)")
    con.execute(f"copy gold to '{config.GOLD_FILE}' (format parquet)")

    bronze_rows = con.execute("select count(*) from bronze").fetchone()[0]
    silver_rows, late_rows = con.execute(
        "select count(*), count(*) filter (where is_late) from silver"
    ).fetchone()
    dead_letters = sum(
        len(p.read_text().splitlines()) for p in config.DEAD_LETTER_DIR.glob("*.jsonl")
    )

    print("== batch run ==")
    print(f"bronze rows read        : {bronze_rows}")
    print(f"duplicates removed      : {bronze_rows - silver_rows}")
    print(f"silver rows written     : {silver_rows}")
    print(f"late events re-dated    : {late_rows}")
    print(f"dead letters (rejected) : {dead_letters}")

    # data quality checks on silver. any failure makes the job exit non-zero,
    # which is what an orchestrator would alert on.
    checks = {
        "event_id is unique": "select count(*) - count(distinct event_id) from silver",
        "no missing keys": "select count(*) from silver where event_id is null or account_id is null",
        "amounts are positive": "select count(*) from silver where amount <= 0",
        "gold adds up to silver": "select (select sum(amount) from silver) - (select sum(amount) from gold)",
    }
    print("\n== data quality checks ==")
    failed = False
    for name, sql in checks.items():
        ok = con.execute(sql).fetchone()[0] == 0
        failed = failed or not ok
        print(f"{'PASS' if ok else 'FAIL'}  {name}")

    # reconciliation: how far have the stream's live totals drifted from the truth?
    print("\n== stream vs batch ==")
    live = json.loads(config.LIVE_TOTALS_FILE.read_text()) if config.LIVE_TOTALS_FILE.exists() else {}
    rows = con.execute("""
        select merchant_category, count(*), sum(amount)
        from silver group by 1 order by 1
    """).fetchall()
    print(f"{'category':<12}{'stream':>8}{'batch':>8}{'extra':>7}{'amount drift':>14}")
    for category, events, amount in rows:
        stream = live.get(category, {"events": 0, "amount": 0.0})
        drift = stream["amount"] - float(amount)
        print(f"{category:<12}{stream['events']:>8}{events:>8}{stream['events'] - events:>7}{drift:>14.2f}")
    print("\n'extra' is what the stream counted more than once. the batch tables are the corrected version.")

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
