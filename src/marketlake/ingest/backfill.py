"""Coinbase REST backfill for trade-id gaps.

Consumes `ingest.gaps`, fetches the missing ids from the public trades
endpoint, and produces them to `trades.raw` exactly like the websocket
ingestor does. A gap's offset is committed only after every fetched trade is
acknowledged by Kafka, so a crash replays the gap; silver dedupes on
(exchange, symbol, trade_id), so a replay adds nothing. Each handled gap
leaves an audit record on `ingest.backfills`.

Run: uv run python -m marketlake.ingest.backfill
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import time
from collections.abc import Iterator
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

import httpx
from confluent_kafka import Consumer, KafkaException, Producer

from marketlake.ingest.coinbase import GAPS_TOPIC, TRADES_TOPIC, make_producer, parse_match
from marketlake.models import Gap, Trade

TRADES_URL = "https://api.exchange.coinbase.com/products/{symbol}/trades"
RESULTS_TOPIC = "ingest.backfills"
PAGE_SIZE = 1000  # the endpoint's maximum
# Public endpoints allow ~10 req/s per IP; stay well under it.
MIN_REQUEST_INTERVAL_S = 0.2
MAX_RETRIES = 8
MAX_RETRY_AFTER_S = 60.0
# A gap this large means corrupt state, not downtime (a day of BTC-USD is ~1M
# trades). It is recorded as skipped instead of paging through years of history.
MAX_GAP_IDS = 5_000_000

log = logging.getLogger("ingest.backfill")


@dataclass(frozen=True, slots=True)
class BackfillResult:
    exchange: str
    symbol: str
    first_missing: int
    last_missing: int
    fetched: int
    # Ids in the range the exchange doesn't have either (e.g. busted trades).
    unavailable: int
    status: str  # "complete" | "partial" | "skipped"
    detected_ts: str
    backfilled_ts: str

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), separators=(",", ":")).encode()


class TradesClient:
    """Pages backwards through Coinbase's public trades endpoint."""

    def __init__(self, client: httpx.Client | None = None,
                 min_interval_s: float = MIN_REQUEST_INTERVAL_S,
                 sleep=time.sleep) -> None:
        self.client = client or httpx.Client(timeout=30, headers={"User-Agent": "marketlake"})
        self.min_interval_s = min_interval_s
        self.sleep = sleep
        self._last_request = 0.0

    def _get(self, symbol: str, after: int) -> list[dict]:
        url = TRADES_URL.format(symbol=symbol)
        params = {"limit": PAGE_SIZE, "after": after}
        backoff = 1.0
        for attempt in range(MAX_RETRIES):
            wait = self._last_request + self.min_interval_s - time.monotonic()
            if wait > 0:
                self.sleep(wait)
            self._last_request = time.monotonic()
            try:
                resp = self.client.get(url, params=params)
            except httpx.TransportError as e:
                log.warning("GET %s after=%d failed (%s); retrying", symbol, after, e)
                self.sleep(backoff)
                backoff = min(backoff * 2, MAX_RETRY_AFTER_S)
                continue
            if resp.status_code == 429 or resp.status_code >= 500:
                try:
                    delay = min(float(resp.headers.get("Retry-After", backoff)), MAX_RETRY_AFTER_S)
                except ValueError:
                    delay = backoff
                log.warning("GET %s after=%d -> %d; retrying in %.1fs",
                            symbol, after, resp.status_code, delay)
                self.sleep(delay)
                backoff = min(backoff * 2, MAX_RETRY_AFTER_S)
                continue
            resp.raise_for_status()
            page = resp.json()
            if not isinstance(page, list):
                raise ValueError(f"unexpected trades payload: {str(page)[:200]}")
            return page
        raise RuntimeError(f"GET {symbol} trades after={after}: gave up after {MAX_RETRIES} attempts")

    def pages(self, symbol: str, first: int, last: int) -> Iterator[list[dict]]:
        """Yields pages of trades with first <= trade_id <= last, newest first.

        `after=N` returns the trades with ids below N in descending order, so
        paging starts just above the gap and walks down until it passes `first`.
        """
        after = last + 1
        while after > first:
            page = self._get(symbol, after)
            if not page:
                return
            yield [t for t in page if first <= int(t["trade_id"]) <= last]
            lowest = min(int(t["trade_id"]) for t in page)
            if lowest >= after:
                raise ValueError(f"{symbol} page after={after} didn't move backwards (min id {lowest})")
            after = lowest


def to_trade(raw: dict, symbol: str, ingest_ts: str) -> Trade:
    # REST trades carry the same maker-side convention as websocket matches.
    return parse_match(raw | {"product_id": symbol}, ingest_ts)


class Backfiller:
    def __init__(self, producer: Producer, client: TradesClient | None = None,
                 flush_timeout: float = 60.0, max_gap_ids: int = MAX_GAP_IDS) -> None:
        self.producer = producer
        self.client = client or TradesClient()
        self.flush_timeout = flush_timeout
        self.max_gap_ids = max_gap_ids

    def backfill(self, gap: Gap) -> BackfillResult:
        """Fetches and produces one gap. Raises if any record isn't acknowledged,
        so the caller leaves the gap uncommitted and it's retried."""
        failed: list[str] = []

        def on_delivery(err, msg) -> None:
            if err is not None:
                failed.append(str(err))

        if gap.size > self.max_gap_ids:
            log.error("gap %s %d..%d is %d ids (> %d); skipping", gap.symbol,
                      gap.first_missing, gap.last_missing, gap.size, self.max_gap_ids)
            fetched, status = 0, "skipped"
        else:
            fetched = 0
            for page in self.client.pages(gap.symbol, gap.first_missing, gap.last_missing):
                ingest_ts = datetime.now(UTC).isoformat()
                for raw in page:
                    t = to_trade(raw, gap.symbol, ingest_ts)
                    self._produce(TRADES_TOPIC, t.key, t.to_json(), on_delivery)
                fetched += len(page)
                self.producer.poll(0)
            status = "complete" if fetched == gap.size else "partial"

        result = BackfillResult(
            exchange=gap.exchange, symbol=gap.symbol, first_missing=gap.first_missing,
            last_missing=gap.last_missing, fetched=fetched,
            unavailable=0 if status == "skipped" else gap.size - fetched,
            status=status, detected_ts=gap.detected_ts,
            backfilled_ts=datetime.now(UTC).isoformat(),
        )
        self._produce(RESULTS_TOPIC, f"{gap.exchange}:{gap.symbol}", result.to_json(), on_delivery)
        remaining = self.producer.flush(self.flush_timeout)
        if remaining or failed:
            raise KafkaException(f"backfill of {gap.symbol} {gap.first_missing}..{gap.last_missing}: "
                                 f"{remaining} unflushed, {len(failed)} failed ({failed[:1]})")
        return result

    def _produce(self, topic: str, key: str, value: bytes, on_delivery) -> None:
        while True:
            try:
                self.producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
                return
            except BufferError:
                # Big gaps outrun the local queue; let it drain.
                self.producer.poll(1.0)

    def run(self, consumer: Consumer, stop=lambda: False) -> None:
        consumer.subscribe([GAPS_TOPIC])
        while not stop():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            try:
                gap = Gap(**json.loads(msg.value()))
            except (json.JSONDecodeError, TypeError) as e:
                # Retrying can't fix a malformed event; log it and move on.
                log.error("skipping malformed gap event at %s[%d]@%d (%s): %.200s",
                          msg.topic(), msg.partition(), msg.offset(), e, msg.value())
            else:
                started = time.monotonic()
                r = self.backfill(gap)
                log.info("backfilled %s %d..%d: %d/%d ids (%s) in %.1fs", r.symbol,
                         r.first_missing, r.last_missing, r.fetched, gap.size, r.status,
                         time.monotonic() - started)
            # Only after the trades are acknowledged: a crash before this line
            # replays the gap, and silver's MERGE drops the duplicates.
            consumer.commit(message=msg, asynchronous=False)


def consumer_conf(bootstrap: str, group: str = "backfill-coinbase") -> dict:
    return {
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        # A large gap can take minutes to page through between polls.
        "max.poll.interval.ms": 30 * 60 * 1000,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:19092")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per page otherwise

    stopping = False

    def _stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    consumer = Consumer(consumer_conf(args.bootstrap))
    try:
        Backfiller(make_producer(args.bootstrap)).run(consumer, stop=lambda: stopping)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
