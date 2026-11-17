"""Stopping the stream lets the in-flight micro-batch finish (#19)."""

import time

import pytest

from marketlake.spark.stream_trades import stop_gracefully


def slow_batch_query(spark, tmp_path, done):
    src = tmp_path / "src"
    spark.range(10).write.parquet(str(src))

    def batch(df, batch_id):
        # A JVM job that takes a while, like the silver MERGE.
        df.sparkSession.range(0, 2, 1, 2).foreach(lambda _: time.sleep(2))
        (done / str(batch_id)).touch()

    q = (spark.readStream.schema("id BIGINT").parquet(str(src)).writeStream
         .foreachBatch(batch).option("checkpointLocation", str(tmp_path / "ckpt"))
         .trigger(processingTime="1 second").start())
    while not q.status["isTriggerActive"]:  # wait until the batch is running
        time.sleep(0.05)
    time.sleep(0.5)
    return q


@pytest.mark.parametrize("graceful", [True, False])
def test_stop_waits_for_in_flight_batch(spark, tmp_path, graceful):
    done = tmp_path / "done"
    done.mkdir()
    q = slow_batch_query(spark, tmp_path, done)
    if graceful:
        stop_gracefully(q)
        assert q.exception() is None
        assert (done / "0").exists()
    else:
        q.stop()  # interrupts the running job: the batch never completes
        assert not (done / "0").exists()
