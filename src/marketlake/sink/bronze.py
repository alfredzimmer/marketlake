"""Kafka `trades.raw` -> bronze Parquet sink.

Delivery guarantee: at-least-once. Offsets are committed only after the
Parquet file holding those records is durably renamed into place. A crash
between write and commit replays the batch; the silver layer dedupes on
(exchange, symbol, trade_id). Each file carries the Kafka partition/offset
range it covers, so replays are auditable.

Layout: data/bronze/trades/exchange=<ex>/date=<YYYY-MM-DD>/p<part>-<first>-<last>.parquet

Records that don't parse or validate are not dropped and don't stop the sink:
they land in exchange=_unparsed/date=<kafka timestamp date> with the raw value
and the error, so bronze stays lossless and a poison pill can't crash-loop it.

Run: uv run python -m marketlake.sink.bronze
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import signal
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, TopicPartition

TOPIC = "trades.raw"

SCHEMA = pa.schema([
    ("exchange", pa.string()),
    ("symbol", pa.string()),
    ("trade_id", pa.int64()),
    ("price", pa.string()),  # kept as exchange-provided strings; typed in silver
    ("size", pa.string()),
    ("taker_side", pa.string()),
    ("event_ts", pa.string()),
    ("ingest_ts", pa.string()),
    ("_kafka_partition", pa.int32()),
    ("_kafka_offset", pa.int64()),
    ("_raw_value", pa.binary()),  # exact bytes; only set for unparsed records
    ("_parse_error", pa.string()),
])

UNPARSED = "_unparsed"
STRING_FIELDS = ("exchange", "symbol", "price", "size", "taker_side", "event_ts", "ingest_ts")
# Values used as path segments; anything else could escape the bronze root.
SAFE_SEGMENT = re.compile(r"[a-z0-9][a-z0-9_.-]*")
DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


def parse_record(value: bytes | None) -> dict:
    """Parses and validates one trades.raw value. Raises ValueError on anything
    that would fail the Parquet write or produce an unsafe partition path."""
    if value is None:
        raise ValueError("null value (tombstone)")
    try:
        d = json.loads(value)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ValueError(f"invalid json: {e}") from None
    if not isinstance(d, dict):
        raise ValueError(f"expected object, got {type(d).__name__}")
    missing = [f for f in (*STRING_FIELDS, "trade_id") if f not in d]
    if missing:
        raise ValueError(f"missing fields: {missing}")
    for f in STRING_FIELDS:
        if not isinstance(d[f], str):
            raise ValueError(f"{f} must be a string")
    if not isinstance(d["trade_id"], int) or isinstance(d["trade_id"], bool):
        raise ValueError("trade_id must be an integer")
    if not -(2**63) <= d["trade_id"] < 2**63:
        raise ValueError("trade_id out of int64 range")
    if not SAFE_SEGMENT.fullmatch(d["exchange"]):
        raise ValueError(f"unsafe exchange name: {d['exchange']!r}")
    if not DATE.match(d["event_ts"]):
        raise ValueError(f"event_ts not ISO-8601: {d['event_ts']!r}")
    return {f: d[f] for f in (*STRING_FIELDS, "trade_id")}

log = logging.getLogger("sink.bronze")


class BronzeSink:
    def __init__(self, consumer: Consumer, root: Path, max_rows: int, max_age_s: float) -> None:
        self.consumer = consumer
        self.root = root
        self.max_rows = max_rows
        self.max_age_s = max_age_s
        # (partition, exchange, date) -> rows
        self._buf: dict[tuple[int, str, str], list[dict]] = defaultdict(list)
        self._next_offsets: dict[int, int] = {}
        self._rows = 0
        self._oldest: float | None = None
        self._running = True
        self.unparsed = 0

    def stop(self, *_) -> None:
        self._running = False

    def add(self, partition: int, offset: int, value: bytes | None, kafka_ts_ms: int) -> None:
        meta = {"_kafka_partition": partition, "_kafka_offset": offset,
                "_raw_value": None, "_parse_error": None}
        try:
            row = parse_record(value) | meta
            key = (partition, row["exchange"], row["event_ts"][:10])
        except ValueError as e:
            self.unparsed += 1
            log.warning("unparsed record at p%d@%d: %s", partition, offset, e)
            row = dict.fromkeys(SCHEMA.names) | meta
            row["_raw_value"] = value
            row["_parse_error"] = str(e)
            date = datetime.fromtimestamp(kafka_ts_ms / 1000, UTC).date().isoformat()
            key = (partition, UNPARSED, date)
        self._buf[key].append(row)
        self._next_offsets[partition] = offset + 1
        self._rows += 1
        if self._oldest is None:
            self._oldest = time.monotonic()

    def should_flush(self) -> bool:
        if self._rows == 0:
            return False
        return self._rows >= self.max_rows or time.monotonic() - self._oldest >= self.max_age_s

    def flush(self) -> None:
        if self._rows == 0:
            return
        for (partition, exchange, date), rows in self._buf.items():
            first, last = rows[0]["_kafka_offset"], rows[-1]["_kafka_offset"]
            out_dir = self.root / "trades" / f"exchange={exchange}" / f"date={date}"
            out_dir.mkdir(parents=True, exist_ok=True)
            final = out_dir / f"p{partition}-{first}-{last}.parquet"
            tmp = final.with_suffix(".parquet.tmp")
            pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), tmp, compression="zstd")
            # fsync + atomic rename: readers never see a half-written file.
            with open(tmp, "rb") as f:
                os.fsync(f.fileno())
            os.replace(tmp, final)

        self.consumer.commit(
            offsets=[TopicPartition(TOPIC, p, o) for p, o in self._next_offsets.items()],
            asynchronous=False,
        )
        log.info("flushed %d rows into %d files; committed %s",
                 self._rows, len(self._buf), dict(self._next_offsets))
        self._buf.clear()
        self._next_offsets.clear()
        self._rows = 0
        self._oldest = None

    def on_revoke(self, consumer, partitions) -> None:
        # Flush before giving up partitions so the next owner doesn't replay
        # a batch we already wrote.
        self.flush()

    def run(self) -> None:
        self.consumer.subscribe([TOPIC], on_revoke=self.on_revoke)
        try:
            while self._running:
                msg = self.consumer.poll(0.5)
                if msg is not None:
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            log.error("consumer error: %s", msg.error())
                    else:
                        self.add(msg.partition(), msg.offset(), msg.value(), msg.timestamp()[1])
                if self.should_flush():
                    self.flush()
            self.flush()
        finally:
            self.consumer.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:19092")
    ap.add_argument("--group", default="bronze-sink")
    ap.add_argument("--root", type=Path, default=Path("data/bronze"))
    ap.add_argument("--max-rows", type=int, default=50_000)
    ap.add_argument("--max-age-s", type=float, default=60.0)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    sink = BronzeSink(consumer, args.root, args.max_rows, args.max_age_s)
    signal.signal(signal.SIGINT, sink.stop)
    signal.signal(signal.SIGTERM, sink.stop)
    sink.run()


if __name__ == "__main__":
    main()
