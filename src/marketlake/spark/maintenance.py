"""Iceberg table maintenance.

Routine (safe while the streaming job runs):
    uv run python -m marketlake.spark.maintenance compact   # small files -> target size
    uv run python -m marketlake.spark.maintenance expire    # old snapshots (#6)
    uv run python -m marketlake.spark.maintenance orphans   # unreferenced files
    uv run python -m marketlake.spark.maintenance all

One-off (stop the streaming job first):
    uv run python -m marketlake.spark.maintenance dedupe-silver
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from pyspark.sql import SparkSession

log = logging.getLogger("spark.maintenance")

CATALOG = "lake"
TABLES = ["silver.trades", "gold.bars_1m", "quarantine.trades"]

# Snapshot retention. Keep a margin of recent snapshots regardless of age: the
# Iceberg streaming sink makes replays idempotent by finding its last committed
# epoch id in the snapshot history. If compaction commits after the stream and
# expiry then keeps only that compaction snapshot, the stream's marker is gone
# and a replayed epoch would be committed again (duplicate gold bars).
RETAIN_LAST = 100
MAX_SNAPSHOT_AGE = timedelta(hours=24)
# Orphan cleanup must not race in-flight writes: a file written by a commit
# that hasn't landed yet looks orphaned. Iceberg's default floor is 3 days.
ORPHAN_MIN_AGE = timedelta(days=3)
TARGET_FILE_BYTES = 128 * 1024 * 1024


@dataclass
class FileStats:
    files: int
    avg_kb: float
    snapshots: int
    manifests: int

    def __str__(self) -> str:
        return (f"files={self.files} avg_kb={self.avg_kb:.1f} "
                f"snapshots={self.snapshots} manifests={self.manifests}")


def file_stats(spark: SparkSession, table: str) -> FileStats:
    f = spark.sql(f"SELECT count(*), coalesce(avg(file_size_in_bytes), 0) FROM {table}.files").first()
    snapshots = spark.sql(f"SELECT count(*) FROM {table}.snapshots").first()[0]
    manifests = spark.sql(f"SELECT count(*) FROM {table}.manifests").first()[0]
    return FileStats(f[0], f[1] / 1024, snapshots, manifests)


def compact(spark: SparkSession, table: str, min_input_files: int = 5) -> None:
    """Bin-packs small data files (streaming micro-batches write many) and
    rewrites manifests. partial-progress commits per file group, so a conflict
    with a concurrent streaming commit only retries that group."""
    spark.sql(f"""
        CALL {CATALOG}.system.rewrite_data_files(
            table => '{table}',
            options => map(
                'target-file-size-bytes', '{TARGET_FILE_BYTES}',
                'min-input-files', '{min_input_files}',
                'partial-progress.enabled', 'true'))
    """)
    spark.sql(f"CALL {CATALOG}.system.rewrite_manifests(table => '{table}')")


def expire(spark: SparkSession, table: str, now: datetime | None = None,
           max_age: timedelta = MAX_SNAPSHOT_AGE, retain_last: int = RETAIN_LAST) -> None:
    cutoff = (now or datetime.now(UTC)) - max_age
    spark.sql(f"""
        CALL {CATALOG}.system.expire_snapshots(
            table => '{table}',
            older_than => TIMESTAMP '{cutoff.strftime("%Y-%m-%d %H:%M:%S")}',
            retain_last => {retain_last})
    """)


def remove_orphans(spark: SparkSession, table: str, now: datetime | None = None,
                   min_age: timedelta = ORPHAN_MIN_AGE) -> None:
    cutoff = (now or datetime.now(UTC)) - min_age
    # prefix_listing lists through the table's FileIO (S3FileIO here); the
    # default Hadoop FileSystem listing has no s3:// implementation configured.
    spark.sql(f"""
        CALL {CATALOG}.system.remove_orphan_files(
            table => '{table}',
            older_than => TIMESTAMP '{cutoff.strftime("%Y-%m-%d %H:%M:%S")}',
            prefix_listing => true)
    """)

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
    Kafka record. Cleans up after #1. Returns (rows_before, rows_after).

    Stop the streaming job first: the overwrite replaces every partition with
    the deduplicated snapshot it read, so trades committed by the stream while
    it runs could be overwritten.
    """
    before = spark.table("silver.trades").count()
    # Dynamic mode replaces only the partitions present in the result (all of
    # them here) instead of truncating the table first.
    previous = spark.conf.get("spark.sql.sources.partitionOverwriteMode", "static")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    try:
        spark.sql(DEDUPE_SILVER)
    finally:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", previous)
    return before, spark.table("silver.trades").count()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("task", choices=["compact", "expire", "orphans", "all", "dedupe-silver"])
    ap.add_argument("--table", action="append", help="default: all tables")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    from marketlake.spark.session import build

    spark = build("marketlake-maintenance")
    if args.task == "dedupe-silver":
        before, after = dedupe_silver(spark)
        print(f"silver.trades: {before} -> {after} rows ({before - after} duplicates removed)")
        return

    steps = {"compact": [compact], "expire": [expire], "orphans": [remove_orphans],
             "all": [compact, expire, remove_orphans]}[args.task]
    for table in args.table or TABLES:
        before = file_stats(spark, table)
        for step in steps:
            step(spark, table)
        log.info("%s: %s -> %s", table, before, file_stats(spark, table))


if __name__ == "__main__":
    main()
