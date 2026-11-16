import json

import pyarrow.dataset as ds
import pytest

from marketlake.sink.bronze import BronzeSink, parse_record

TS_MS = 1794783789910  # 2026-11-15 UTC


class FakeConsumer:
    def __init__(self):
        self.commits = []

    def commit(self, offsets, asynchronous):
        self.commits.append({tp.partition: tp.offset for tp in offsets})


def good(trade_id, **overrides):
    d = {"exchange": "coinbase", "symbol": "BTC-USD", "trade_id": trade_id, "price": "1.5",
         "size": "2", "taker_side": "buy", "event_ts": "2026-11-15T22:00:00.000001Z",
         "ingest_ts": "2026-11-15T22:00:00.1+00:00"}
    return json.dumps(d | overrides).encode()


@pytest.mark.parametrize("value, error", [
    (b"not-json{", "invalid json"),
    (b"\xff\xfe", "invalid json"),
    (b"[1, 2]", "expected object"),
    (json.dumps({"exchange": "coinbase"}).encode(), "missing fields"),
    (good("7"), "trade_id must be an integer"),
    (good(True), "trade_id must be an integer"),
    (good(2**63), "out of int64 range"),
    (good(1, price=1.5), "price must be a string"),
    (good(1, exchange="../../etc"), "unsafe exchange name"),
    (good(1, event_ts="yesterday"), "event_ts not ISO-8601"),
])
def test_parse_record_rejects(value, error):
    with pytest.raises(ValueError, match=error):
        parse_record(value)


def test_poison_pill_is_quarantined_not_fatal(tmp_path):
    consumer = FakeConsumer()
    sink = BronzeSink(consumer, tmp_path, max_rows=100, max_age_s=60)
    sink.add(0, 0, good(1), TS_MS)
    sink.add(0, 1, b"not-json{", TS_MS)
    sink.add(0, 2, good(2, exchange="../../escape"), TS_MS)
    sink.add(0, 3, good(3), TS_MS)
    sink.flush()

    assert consumer.commits == [{0: 4}]  # offsets advance past the bad records
    assert sink.unparsed == 2
    assert not (tmp_path.parent / "escape").exists()

    t = ds.dataset(tmp_path / "trades", format="parquet", partitioning="hive").to_table()
    rows = sorted(t.to_pylist(), key=lambda r: r["_kafka_offset"])
    assert [r["exchange"] for r in rows] == ["coinbase", "_unparsed", "_unparsed", "coinbase"]
    assert rows[1]["_raw_value"] == "not-json{"
    assert "invalid json" in rows[1]["_parse_error"]
    assert rows[1]["date"] == "2026-11-15"
    assert rows[0]["_raw_value"] is None and rows[0]["trade_id"] == 1
