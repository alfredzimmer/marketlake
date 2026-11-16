import json

import pytest

from marketlake.spark.stream_trades import parse_kafka, write_silver
from marketlake.spark.tables import create_all
from test_spark_transforms import kafka_rows, trade


def without(field, **kw):
    t = trade(kw.pop("trade_id", 9), "100", "1", "buy", "2026-11-15T22:00:00Z")
    del t[field]
    return json.dumps(t).encode()


def batch(spark, good, bad):
    return parse_kafka(kafka_rows(spark, good, extra_values=bad))


@pytest.fixture
def tables(spark):
    create_all(spark)
    spark.sql("DELETE FROM silver.trades")
    spark.sql("DELETE FROM quarantine.trades")


def test_every_required_column_is_validated(spark):
    bad = [b"not-json{", b"[1,2]"] + [without(f) for f in
           ("exchange", "symbol", "trade_id", "price", "size", "taker_side", "event_ts", "ingest_ts")]
    reasons = [r.reject_reason for r in batch(spark, [], bad).orderBy("kafka_offset").collect()]
    assert reasons == [
        "invalid json", "missing or invalid exchange",  # valid JSON, but not a trade
        "missing or invalid exchange", "missing or invalid symbol", "missing or invalid trade_id",
        "missing or invalid price", "missing or invalid size", "missing or invalid taker_side",
        "missing or invalid event_ts", "missing or invalid ingest_ts",
    ]


def test_rejects_are_quarantined_not_fatal_and_replay_safe(spark, tables):
    good = [trade(1, "100", "1", "buy", "2026-11-15T22:00:00Z")]
    bad = [b"not-json{", without("exchange")]  # missing exchange used to fail the write
    b = batch(spark, good, bad)

    write_silver(b, 0)
    write_silver(b, 0)  # replay

    assert spark.table("silver.trades").count() == 1
    q = spark.table("quarantine.trades").orderBy("kafka_offset").collect()
    assert [(r.kafka_offset, r.reject_reason) for r in q] == [
        (1, "invalid json"), (2, "missing or invalid exchange")]
    assert q[0].raw_value == b"not-json{"
