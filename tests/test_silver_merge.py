from marketlake.spark.stream_trades import SILVER_COLUMNS, merge_silver, parse_trades
from marketlake.spark.tables import create_all
from test_spark_transforms import kafka_rows, trade


def silver_batch(spark):
    trades = [
        trade(1, "100", "1", "buy", "2026-11-15T22:00:10Z"),
        trade(2, "101", "1", "sell", "2026-11-15T22:30:00Z"),
        trade(3, "102", "1", "buy", "2026-11-15T23:59:59.999999Z"),
    ]
    return parse_trades(kafka_rows(spark, trades)).select(*SILVER_COLUMNS)


def test_replayed_batch_inserts_nothing(spark):
    create_all(spark)
    spark.sql("DELETE FROM silver.trades")
    batch = silver_batch(spark)

    merge_silver(batch, 0)
    merge_silver(batch, 0)  # replay after a crash before the checkpoint commit

    n, distinct = spark.sql(
        "SELECT count(*), count(DISTINCT exchange, symbol, trade_id) FROM silver.trades").first()
    assert (n, distinct) == (3, 3)


def test_dedupe_silver_keeps_earliest_record(spark):
    from marketlake.spark.maintenance import dedupe_silver

    create_all(spark)
    spark.sql("DELETE FROM silver.trades")
    batch = silver_batch(spark)
    batch.writeTo("silver.trades").append()
    batch.writeTo("silver.trades").append()  # simulate the duplicates #1 produced

    assert dedupe_silver(spark) == (6, 3)
    assert dedupe_silver(spark) == (3, 3)  # idempotent
