"""Structured Streaming: Kafka `trades.raw` -> Iceberg silver.trades + gold.bars_1m.

Two queries share one parsing step:

silver  Every valid trade, deduplicated on (exchange, symbol, trade_id).
        Written with foreachBatch + MERGE INTO ... WHEN NOT MATCHED INSERT, so
        replaying a micro-batch (after a crash between the Iceberg commit and the
        checkpoint commit) inserts nothing new: idempotent, effectively once.
        No watermark: late trades are always accepted here. Records failing
        parsing/validation go to quarantine.trades (raw bytes + reason, MERGE on
        Kafka partition/offset) instead of being dropped.

gold    1-minute OHLCV + VWAP bars per (exchange, symbol), event-time windows with
        a watermark. Append mode emits each bar exactly once, after the watermark
        passes the window end; trades arriving later than that are dropped from
        the streaming bars (and counted). Phase 4 reconciles these against a
        batch recompute from silver. The Iceberg streaming sink records the epoch
        id in each snapshot and skips epochs it already committed.
        One query per exchange: Spark keeps a single watermark per query, so a
        shared query would let the fastest feed push it forward and drop a
        lagging exchange's trades as late (#9).

Run: uv run python -m marketlake.spark.stream_trades
"""

from __future__ import annotations

import argparse
import logging
import os
import signal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import LongType, StringType, StructField, StructType

from marketlake.spark.session import build
from marketlake.spark.tables import create_all

log = logging.getLogger("spark.stream_trades")

KEY = ["exchange", "symbol", "trade_id"]
PRICE = "decimal(28,10)"
AMOUNT = "decimal(38,10)"
# Narrower operand type for price*size: decimal(28,10)*decimal(28,10) would
# overflow 38 digits and Spark would cut the result scale to 6. decimal(20,10)
# operands give a decimal(38,17) product.
OPERAND = "decimal(20,10)"

TRADE_JSON = StructType([
    StructField("exchange", StringType()),
    StructField("symbol", StringType()),
    StructField("trade_id", LongType()),
    StructField("price", StringType()),
    StructField("size", StringType()),
    StructField("taker_side", StringType()),
    StructField("event_ts", StringType()),
    StructField("ingest_ts", StringType()),
])

REQUIRED = ["exchange", "symbol", "trade_id", "price", "size", "taker_side",
            "event_ts", "ingest_ts"]
QUARANTINE_COLUMNS = ["kafka_partition", "kafka_offset", "kafka_ts", "raw_value",
                      "reject_reason", "quarantined_at"]
SILVER_COLUMNS = ["exchange", "symbol", "trade_id", "price", "size", "taker_side",
                  "event_ts", "ingest_ts", "kafka_partition", "kafka_offset"]


def read_kafka(spark: SparkSession, bootstrap: str, max_offsets: int) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", "trades.raw")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )


def parse_kafka(kafka_df: DataFrame) -> DataFrame:
    """Parses every Kafka record and tags it with `reject_reason` (null when
    the record is valid). Keeps the raw value and Kafka coordinates so rejects
    can be quarantined."""
    raw = F.col("value").cast("string")
    t = F.from_json(raw, TRADE_JSON)
    parsed = kafka_df.select(
        t.alias("t"),
        F.col("value").alias("raw_value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.get_json_object(raw, "$").isNotNull().alias("_is_json"),
    ).select(
        "t.exchange",
        "t.symbol",
        "t.trade_id",
        # try_cast: Spark 4 runs in ANSI mode, where a plain cast of one
        # malformed value throws and kills the stream.
        F.col("t.price").try_cast(PRICE).alias("price"),
        F.col("t.size").try_cast(PRICE).alias("size"),
        "t.taker_side",
        F.col("t.event_ts").try_cast("timestamp").alias("event_ts"),
        F.col("t.ingest_ts").try_cast("timestamp").alias("ingest_ts"),
        "kafka_partition",
        "kafka_offset",
        "kafka_ts",
        "raw_value",
        "_is_json",
    )
    # Every NOT NULL silver column must be checked: one null in a required
    # Iceberg column fails the whole write, and the stream would then fail on
    # the same record after every restart (#7).
    reason = F.when(~F.col("_is_json"), F.lit("invalid json"))
    for col in REQUIRED:
        reason = reason.when(F.col(col).isNull(), F.lit(f"missing or invalid {col}"))
    return parsed.withColumn("reject_reason", reason).drop("_is_json")


def parse_trades(kafka_df: DataFrame) -> DataFrame:
    """Valid trades only."""
    return parse_kafka(kafka_df).where(F.col("reject_reason").isNull()).select(*SILVER_COLUMNS)


def merge_silver(batch: DataFrame, batch_id: int) -> None:
    batch = batch.dropDuplicates(KEY).persist()
    try:
        # Render the bounds as strings inside Spark (session TZ = UTC). Collecting
        # them as Python datetimes would convert to the *process's* local zone,
        # and the TIMESTAMP literals below would then be off by the UTC offset,
        # so the MERGE would never match and replays would duplicate rows (#1).
        lo, hi = batch.agg(F.min("event_ts").cast("string"),
                           F.max("event_ts").cast("string")).first()
        if lo is None:
            return
        batch.createOrReplaceTempView("silver_batch")
        # Spark disables AQE in the session micro-batches run on. Without it the
        # MERGE's sort-order range shuffle writes one file per shuffle
        # partition: 8 files for a 16-row batch, ~69k files/day at a 10s
        # trigger. The MERGE is an ordinary batch query, so turn AQE back on
        # and let it coalesce toward the table's advisory partition size.
        batch.sparkSession.conf.set("spark.sql.adaptive.enabled", "true")
        # The event_ts bound on the target lets Iceberg prune to the day
        # partitions this batch touches instead of scanning the whole table.
        batch.sparkSession.sql(f"""
            MERGE INTO silver.trades t
            USING silver_batch s
            ON  t.exchange = s.exchange AND t.symbol = s.symbol AND t.trade_id = s.trade_id
            AND t.event_ts BETWEEN TIMESTAMP '{lo}' AND TIMESTAMP '{hi}'
            WHEN NOT MATCHED THEN INSERT *
        """)
    finally:
        batch.unpersist()


def quarantine_rejects(rejects: DataFrame) -> None:
    rejects.createOrReplaceTempView("quarantine_batch")
    rejects.sparkSession.sql("""
        MERGE INTO quarantine.trades t
        USING quarantine_batch s
        ON t.kafka_partition = s.kafka_partition AND t.kafka_offset = s.kafka_offset
        WHEN NOT MATCHED THEN INSERT *
    """)


def write_silver(batch: DataFrame, batch_id: int) -> None:
    """foreachBatch sink: valid rows MERGE into silver, rejects into quarantine."""
    batch = batch.persist()
    try:
        rejects = (batch.where(F.col("reject_reason").isNotNull())
                   .withColumn("quarantined_at", F.current_timestamp())
                   .select(*QUARANTINE_COLUMNS)).persist()
        n_rejected = rejects.count()
        if n_rejected:
            quarantine_rejects(rejects)
            log.warning("silver batch=%d quarantined=%d", batch_id, n_rejected)
        rejects.unpersist()
        merge_silver(batch.where(F.col("reject_reason").isNull()).select(*SILVER_COLUMNS),
                     batch_id)
    finally:
        batch.unpersist()


def bars_1m(trades: DataFrame, watermark: str) -> DataFrame:
    price = F.col("price").cast(OPERAND)
    size = F.col("size").cast(OPERAND)
    # Same function serves the stream and batch recomputes (reconciliation).
    # In batch there is no watermark, so dedupe over the whole input.
    if trades.isStreaming:
        deduped = trades.withWatermark("event_ts", watermark).dropDuplicatesWithinWatermark(KEY)
    else:
        deduped = trades.dropDuplicates(KEY)
    agg = (
        deduped
        .groupBy(F.window("event_ts", "1 minute"), "exchange", "symbol")
        .agg(
            F.min_by("price", "trade_id").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", "trade_id").alias("close"),
            F.sum("size").cast(AMOUNT).alias("volume"),
            F.sum(price * size).cast(AMOUNT).alias("notional"),
            F.count("*").alias("trade_count"),
            F.sum(F.when(F.col("taker_side") == "buy", F.col("size")).otherwise(0)).cast(AMOUNT).alias("buy_volume"),
            F.sum(F.when(F.col("taker_side") == "sell", F.col("size")).otherwise(0)).cast(AMOUNT).alias("sell_volume"),
        )
    )
    return agg.select(
        "exchange",
        "symbol",
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "open", "high", "low", "close", "volume", "notional",
        (F.col("notional") / F.col("volume")).cast(PRICE).alias("vwap"),
        "trade_count", "buy_volume", "sell_volume",
    )


def gold_bars_by_exchange(trades: DataFrame, exchanges: list[str],
                          watermark: str) -> dict[str, DataFrame]:
    """One bars stream per exchange. Filtering before `bars_1m` puts the
    watermark after the filter, so each exchange's watermark follows only its
    own event times."""
    return {ex: bars_1m(trades.where(F.col("exchange") == ex), watermark) for ex in exchanges}


def gold_checkpoint(checkpoints: str, exchange: str) -> str:
    return os.path.join(checkpoints, f"gold_bars_1m_{exchange}")


def migrate_gold_checkpoint(checkpoints: str) -> None:
    """Before #9 one gold query (checkpoint `gold_bars_1m`) covered coinbase,
    the only exchange. Starting the coinbase query on a fresh checkpoint would
    get a new query id, reread Kafka from the earliest offset, and append every
    existing bar again: the Iceberg sink's replay check is per query id. The
    id lives in the checkpoint's metadata, so renaming the directory keeps it,
    and the added exchange filter is a compatible plan change."""
    legacy = os.path.join(checkpoints, "gold_bars_1m")
    target = gold_checkpoint(checkpoints, "coinbase")
    if os.path.isdir(legacy) and not os.path.exists(target):
        os.rename(legacy, target)
        log.info("moved gold checkpoint %s -> %s", legacy, target)


class ProgressLogger(StreamingQueryListener):
    """Logs per-batch throughput and rows dropped as too late by the watermark."""

    def onQueryStarted(self, event) -> None:
        log.info("query %s started (id=%s)", event.name, event.id)

    def onQueryProgress(self, event) -> None:
        p = event.progress
        if p.numInputRows == 0:
            return
        dropped = sum(op.numRowsDroppedByWatermark for op in p.stateOperators)
        log.info(
            "%s batch=%d rows=%d rate=%.0f/s duration=%dms watermark=%s late_dropped=%d",
            p.name, p.batchId, p.numInputRows, p.processedRowsPerSecond or 0,
            p.batchDuration, p.eventTime.get("watermark", "-"), dropped,
        )

    def onQueryIdle(self, event) -> None:
        pass

    def onQueryTerminated(self, event) -> None:
        log.info("query %s terminated%s", event.id, f": {event.exception}" if event.exception else "")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:19092")
    ap.add_argument("--checkpoints", default="data/checkpoints")
    ap.add_argument("--trigger", default="10 seconds")
    ap.add_argument("--watermark", default="30 seconds")
    ap.add_argument("--max-offsets", type=int, default=50_000)
    # Trades from an exchange not listed here reach silver but get no
    # streaming bars (rpt_bar_reconciliation shows them as missing_in_stream).
    ap.add_argument("--exchanges", nargs="+", default=["coinbase"])
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    spark = build("marketlake-stream-trades")
    # Source columns are nullable even after the filter above; Iceberg would
    # otherwise reject writes into the NOT NULL silver columns.
    spark.conf.set("spark.sql.iceberg.check-nullability", "false")
    spark.streams.addListener(ProgressLogger())
    create_all(spark)

    records = parse_kafka(read_kafka(spark, args.bootstrap, args.max_offsets))
    trades = records.where(F.col("reject_reason").isNull()).select(*SILVER_COLUMNS)

    silver = (
        records.writeStream.queryName("silver_trades")
        .foreachBatch(write_silver)
        .option("checkpointLocation", os.path.join(args.checkpoints, "silver_trades"))
        .trigger(processingTime=args.trigger)
        .start()
    )
    migrate_gold_checkpoint(args.checkpoints)
    golds = [
        bars.writeStream.queryName(f"gold_bars_1m_{ex}")
        .format("iceberg").outputMode("append")
        .option("checkpointLocation", gold_checkpoint(args.checkpoints, ex))
        .trigger(processingTime=args.trigger)
        .toTable("gold.bars_1m")
        for ex, bars in gold_bars_by_exchange(trades, args.exchanges, args.watermark).items()
    ]

    # Don't call into the JVM from the signal handler: the main thread is
    # blocked inside a py4j call, and re-entering the gateway breaks it.
    stopping = False

    def request_stop(*_) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    while not stopping and not spark.streams.awaitAnyTermination(timeout=1):
        pass
    log.info("stopping queries")
    for q in (silver, *golds):
        q.stop()
    spark.stop()


if __name__ == "__main__":
    main()
