from marketlake.spark.stream_trades import SILVER_COLUMNS, merge_silver, parse_trades
from marketlake.spark.tables import create_all
from test_spark_transforms import kafka_rows, trade


def test_streaming_micro_batch_writes_one_file_per_partition(spark, tmp_path):
    """Spark disables AQE for streaming micro-batches, so inside foreachBatch a
    tiny batch would otherwise be written as one file per shuffle partition."""
    spark.conf.set("spark.sql.shuffle.partitions", "8")  # as in the live session
    try:
        create_all(spark)
        spark.sql("DELETE FROM silver.trades")
        trades = [trade(i, "100", "1", "buy", f"2026-11-15T22:00:{i:02d}Z") for i in range(40)]
        src = tmp_path / "src"
        kafka_rows(spark, trades).repartition(4).write.parquet(str(src))

        q = (spark.readStream.schema(spark.read.parquet(str(src)).schema).parquet(str(src))
             .writeStream
             .foreachBatch(lambda b, i: merge_silver(parse_trades(b).select(*SILVER_COLUMNS), i))
             .option("checkpointLocation", str(tmp_path / "ckpt"))
             .trigger(availableNow=True).start())
        q.awaitTermination()
        assert q.exception() is None
        # The stream ran on a cloned session with its own catalog cache.
        spark.sql("REFRESH TABLE silver.trades")

        assert spark.table("silver.trades").count() == 40
        added = spark.sql("""SELECT summary['added-data-files'] FROM silver.trades.snapshots
                             ORDER BY committed_at DESC LIMIT 1""").first()[0]
        assert int(added) == 1  # one day/exchange partition -> one file
    finally:
        spark.conf.set("spark.sql.shuffle.partitions", "2")
