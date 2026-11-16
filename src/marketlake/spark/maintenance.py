"""One-off table maintenance.

Run: uv run python -m marketlake.spark.maintenance dedupe-silver
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

DEDUPE_SILVER = """
    INSERT OVERWRITE silver.trades
    SELECT exchange, symbol, trade_id, price, size, taker_side,
           event_ts, ingest_ts, kafka_partition, kafka_offset
    FROM (
        SELECT *, row_number() OVER (
            PARTITION BY exchange, symbol, trade_id ORDER BY kafka_partition, kafka_offset
        ) AS rn
        FROM silver.trades
    )
    WHERE rn = 1
"""


def dedupe_silver(spark: SparkSession) -> tuple[int, int]:
    """Removes duplicate (exchange, symbol, trade_id) rows, keeping the earliest
    Kafka record. Cleans up after #1. Returns (rows_before, rows_after)."""
    before = spark.table("silver.trades").count()
    # Dynamic overwrite only rewrites the day/exchange partitions present in
    # the result, which is all of them here, but keeps the operation
    # partition-scoped rather than a full-table truncate.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.sql(DEDUPE_SILVER)
    return before, spark.table("silver.trades").count()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("task", choices=["dedupe-silver"])
    args = ap.parse_args()

    from marketlake.spark.session import build

    spark = build("marketlake-maintenance")
    if args.task == "dedupe-silver":
        before, after = dedupe_silver(spark)
        print(f"silver.trades: {before} -> {after} rows ({before - after} duplicates removed)")


if __name__ == "__main__":
    main()
