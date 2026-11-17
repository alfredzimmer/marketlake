"""Per-exchange watermarks: a lagging exchange's trades aren't dropped as late
because a faster exchange pushed the watermark forward (#9)."""

import os

from pyspark.sql import functions as F

from marketlake.spark.stream_trades import (bars_1m, gold_bars_by_exchange,
                                            migrate_gold_checkpoint, parse_trades)
from test_spark_transforms import kafka_rows, trade


def on(exchange, trade_id, ts):
    return trade(trade_id, "100", "1", "buy", ts) | {"exchange": exchange}


# One micro-batch per list. Coinbase races ahead to 22:05; binance's feed lags
# and delivers 22:00:40 two batches later. (Late rows are filtered against the
# previous batch's watermark, so a lag of a single batch would still get in.)
BATCHES = [
    [on("coinbase", 1, "2026-11-15T22:00:10Z"), on("binance", 1, "2026-11-15T22:00:10Z")],
    [on("coinbase", 2, "2026-11-15T22:05:00Z")],
    [on("coinbase", 3, "2026-11-15T22:05:10Z")],
    [on("binance", 2, "2026-11-15T22:00:40Z")],
    [on("binance", 3, "2026-11-15T22:05:00Z")],
]


def run(spark, tmp_path, build):
    src = tmp_path / "src"
    for i, batch in enumerate(BATCHES):
        kafka_rows(spark, batch).coalesce(1).write.parquet(str(src / f"b{i}"))
    schema = spark.read.parquet(str(src / "b0")).schema
    trades = parse_trades(
        spark.readStream.schema(schema).option("maxFilesPerTrigger", 1)
        .option("recursiveFileLookup", "true").parquet(str(src)))
    names = []
    for name, bars in build(trades).items():
        q = (bars.writeStream.format("memory").queryName(f"{name}_{tmp_path.name}")
             .outputMode("append").option("checkpointLocation", str(tmp_path / name)).start())
        q.processAllAvailable()
        q.stop()
        names.append(q.name)
    rows = []
    for n in names:
        rows += spark.table(n).where(F.col("window_start") == "2026-11-15 22:00:00").collect()
    return {r.exchange: r.trade_count for r in rows}


def test_shared_watermark_drops_the_lagging_exchange(spark, tmp_path):
    # The pre-#9 shape, kept to show the failure the fix prevents.
    assert run(spark, tmp_path, lambda t: {"all": bars_1m(t, "30 seconds")}) == {
        "coinbase": 1, "binance": 1}


def test_per_exchange_watermarks_keep_the_lagging_exchange(spark, tmp_path):
    got = run(spark, tmp_path,
              lambda t: gold_bars_by_exchange(t, ["coinbase", "binance"], "30 seconds"))
    assert got == {"coinbase": 1, "binance": 2}


def test_legacy_gold_checkpoint_moves_to_coinbase(tmp_path):
    (tmp_path / "gold_bars_1m" / "offsets").mkdir(parents=True)
    (tmp_path / "gold_bars_1m" / "metadata").write_text('{"id":"q-1"}')
    migrate_gold_checkpoint(str(tmp_path))
    assert not (tmp_path / "gold_bars_1m").exists()
    assert (tmp_path / "gold_bars_1m_coinbase" / "metadata").read_text() == '{"id":"q-1"}'
    migrate_gold_checkpoint(str(tmp_path))  # idempotent
    assert sorted(os.listdir(tmp_path)) == ["gold_bars_1m_coinbase"]
