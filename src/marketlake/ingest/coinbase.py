"""Coinbase Exchange websocket -> Kafka trade ingestor.

Subscribes to the public `matches` channel, normalizes each fill into a
`Trade`, and produces it keyed by exchange:symbol so a symbol's trades stay
ordered within one partition. Trade-id gaps (from disconnects or dropped
messages) are published to `ingest.gaps` for later REST backfill.

Run: uv run python -m marketlake.ingest.coinbase --symbols BTC-USD ETH-USD
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import signal
import time
from datetime import UTC, datetime

import websockets
from confluent_kafka import Producer
from websockets.exceptions import InvalidStatus, WebSocketException

from marketlake.ingest.gaps import GapDetector
from marketlake.models import Trade

WS_URL = "wss://ws-feed.exchange.coinbase.com"
EXCHANGE = "coinbase"
TRADES_TOPIC = "trades.raw"
GAPS_TOPIC = "ingest.gaps"
# Cap on a server-requested wait, so a bogus header can't park us for hours.
MAX_RETRY_AFTER_S = 300.0

log = logging.getLogger("ingest.coinbase")


def parse_match(msg: dict, ingest_ts: str) -> Trade:
    # Coinbase reports the *maker* side; the taker (aggressor) is the opposite.
    taker_side = "buy" if msg["side"] == "sell" else "sell"
    return Trade(
        exchange=EXCHANGE,
        symbol=msg["product_id"],
        trade_id=int(msg["trade_id"]),
        price=msg["price"],
        size=msg["size"],
        taker_side=taker_side,
        event_ts=msg["time"],
        ingest_ts=ingest_ts,
    )


def retry_after(e: Exception) -> float:
    """Seconds requested by a 429/503 Retry-After header, else 0."""
    if not isinstance(e, InvalidStatus):
        return 0.0
    try:
        seconds = float(e.response.headers.get("Retry-After", 0))
    except ValueError:  # HTTP-date form; fall back to our own backoff
        return 0.0
    return min(seconds, MAX_RETRY_AFTER_S)


class Stats:
    def __init__(self) -> None:
        self.trades = self.dupes = self.gaps = self.missing = self.reconnects = 0
        self.malformed = 0
        self._t0 = time.monotonic()

    def report(self) -> None:
        elapsed = time.monotonic() - self._t0
        log.info(
            "trades=%d (%.1f/s) dupes=%d gaps=%d missing_ids=%d malformed=%d reconnects=%d",
            self.trades, self.trades / max(elapsed, 1e-9), self.dupes,
            self.gaps, self.missing, self.malformed, self.reconnects,
        )


class CoinbaseIngestor:
    def __init__(self, symbols: list[str], producer: Producer, ws_url: str = WS_URL,
                 initial_backoff: float = 1.0, max_backoff: float = 60.0) -> None:
        self.symbols = symbols
        self.producer = producer
        self.ws_url = ws_url
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        # Lives across reconnects, so a disconnect shows up as a gap.
        self.detector = GapDetector(EXCHANGE)
        self.stats = Stats()
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            log.error("delivery failed for key=%s: %s", msg.key(), err)

    def handle(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
            kind = msg.get("type")
        except (json.JSONDecodeError, AttributeError):
            self._malformed(raw, "not a JSON object")
            return
        if kind == "error":
            raise RuntimeError(f"coinbase error: {msg}")
        if kind not in ("match", "last_match"):
            return

        try:
            trade = parse_match(msg, datetime.now(UTC).isoformat())
        except (KeyError, TypeError, ValueError) as e:
            # One bad message (e.g. an upstream schema change) must not kill
            # the ingestor; count it so a spike is visible.
            self._malformed(raw, f"{type(e).__name__}: {e}")
            return
        is_new, gap = self.detector.observe(trade.symbol, trade.trade_id)
        if not is_new:
            self.stats.dupes += 1
        if gap is not None:
            self.stats.gaps += 1
            self.stats.missing += gap.size
            log.warning("gap %s ids %d..%d (%d missing)", gap.symbol,
                        gap.first_missing, gap.last_missing, gap.size)
            self.producer.produce(GAPS_TOPIC, key=trade.key, value=gap.to_json())

        self.producer.produce(TRADES_TOPIC, key=trade.key, value=trade.to_json(),
                              on_delivery=self._on_delivery)
        self.producer.poll(0)
        self.stats.trades += 1

    def _malformed(self, raw: str, reason: str) -> None:
        self.stats.malformed += 1
        log.warning("skipping malformed message (%s): %.200s", reason, raw)

    async def _session(self) -> None:
        async with websockets.connect(self.ws_url, ping_interval=20, max_queue=4096) as ws:
            await ws.send(json.dumps({
                "type": "subscribe",
                "product_ids": self.symbols,
                "channels": ["matches", "heartbeat"],
            }))
            log.info("subscribed to %s", self.symbols)
            async for raw in ws:
                self.handle(raw)
                if self._stop.is_set():
                    return

    async def run(self) -> None:
        backoff = self.initial_backoff
        reporter = asyncio.create_task(self._report_loop())
        try:
            while not self._stop.is_set():
                try:
                    await self._session()
                    backoff = self.initial_backoff
                except (WebSocketException, OSError, RuntimeError) as e:
                    # WebSocketException covers ConnectionClosed and handshake
                    # rejections (InvalidStatus on 429/5xx during maintenance).
                    self.stats.reconnects += 1
                    delay = max(backoff + random.uniform(0, backoff / 2), retry_after(e))
                    log.warning("connection lost (%s); reconnecting in %.1fs", e, delay)
                    try:
                        await asyncio.wait_for(self._stop.wait(), timeout=delay)
                    except TimeoutError:
                        pass
                    backoff = min(backoff * 2, self.max_backoff)
        finally:
            reporter.cancel()
            remaining = self.producer.flush(10)
            if remaining:
                log.error("%d messages not delivered on shutdown", remaining)
            self.stats.report()

    async def _report_loop(self) -> None:
        while True:
            await asyncio.sleep(30)
            self.stats.report()


def make_producer(bootstrap: str) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap,
        # Idempotent producer: broker dedupes our retries, so a transient
        # network error can't write the same message twice into the log.
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 20,
        "compression.type": "zstd",
    })


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="+", default=["BTC-USD", "ETH-USD", "SOL-USD"])
    ap.add_argument("--bootstrap", default="localhost:19092")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    ingestor = CoinbaseIngestor(args.symbols, make_producer(args.bootstrap))

    async def _main() -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, ingestor.stop)
        await ingestor.run()

    asyncio.run(_main())


if __name__ == "__main__":
    main()
