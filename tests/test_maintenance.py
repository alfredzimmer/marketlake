import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from marketlake.spark.maintenance import compact, expire, file_stats, remove_orphans

TABLE = "maint.events"


@pytest.fixture
def table(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS maint")
    spark.sql(f"DROP TABLE IF EXISTS {TABLE} PURGE")
    spark.sql(f"CREATE TABLE {TABLE} (id BIGINT, v STRING) USING iceberg")
    for i in range(6):  # six micro-batch-sized appends -> six small files
        spark.sql(f"INSERT INTO {TABLE} VALUES ({i}, 'x')")
    yield TABLE
    spark.sql(f"DROP TABLE IF EXISTS {TABLE} PURGE")


def data_dir(spark, table):
    path = spark.sql(f"SELECT file_path FROM {table}.files LIMIT 1").first()[0]
    return Path(path.removeprefix("file:")).parent


def test_compact_merges_small_files_and_keeps_rows(spark, table):
    assert file_stats(spark, table).files == 6
    compact(spark, table, min_input_files=2)
    assert file_stats(spark, table).files == 1
    assert spark.table(table).count() == 6


def test_expire_keeps_retain_last_even_when_all_are_old(spark, table):
    assert file_stats(spark, table).snapshots == 6
    expire(spark, table, now=datetime.now(UTC) + timedelta(days=2), retain_last=3)
    assert file_stats(spark, table).snapshots == 3
    assert spark.table(table).count() == 6


def test_remove_orphans_respects_min_age(spark, table):
    stray = data_dir(spark, table) / "stray-from-failed-write.parquet"
    stray.write_bytes(b"not referenced by any snapshot")

    remove_orphans(spark, table)  # stray is brand new: could be an in-flight write
    assert stray.exists()

    old = time.time() - 4 * 86400
    os.utime(stray, (old, old))
    remove_orphans(spark, table)
    assert not stray.exists()
    assert spark.table(table).count() == 6


def test_streaming_replay_stays_idempotent_after_compaction_and_expiry(spark, tmp_path):
    """The Iceberg streaming sink skips replayed epochs by finding its last epoch
    id in the snapshot history. Compaction commits after the stream; if expiry
    then keeps only that snapshot, the marker is lost and the replay is appended
    again (observed: 35 rows instead of 30 with retain_last=1). RETAIN_LAST
    keeps the marker."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS maint")
    spark.sql("DROP TABLE IF EXISTS maint.sink PURGE")
    spark.sql("CREATE TABLE maint.sink (id BIGINT) USING iceberg")
    src, ckpt = tmp_path / "src", tmp_path / "ckpt"
    for i in range(3):
        spark.range(i * 10, i * 10 + 10).write.mode("append").parquet(str(src))

    def run_stream():
        (spark.readStream.schema("id BIGINT").option("maxFilesPerTrigger", 1).parquet(str(src))
         .writeStream.format("iceberg").outputMode("append")
         .option("checkpointLocation", str(ckpt)).trigger(availableNow=True)
         .toTable("maint.sink").awaitTermination())

    run_stream()
    spark.sql("REFRESH TABLE maint.sink")
    compact(spark, "maint.sink", min_input_files=2)
    expire(spark, "maint.sink", now=datetime.now(UTC) + timedelta(days=2))  # default retain_last

    last = max(int(f) for f in os.listdir(ckpt / "commits") if f.isdigit())
    (ckpt / "commits" / str(last)).unlink()  # crash after the Iceberg commit,
    (ckpt / "commits" / f".{last}.crc").unlink()  # before the checkpoint commit
    run_stream()

    spark.sql("REFRESH TABLE maint.sink")
    assert spark.table("maint.sink").count() == 30
    spark.sql("DROP TABLE maint.sink PURGE")
