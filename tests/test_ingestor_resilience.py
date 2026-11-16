import asyncio
import json
import time
from http import HTTPStatus

from websockets.asyncio.server import serve
from websockets.datastructures import Headers

from marketlake.ingest.coinbase import CoinbaseIngestor


class FakeProducer:
    def __init__(self):
        self.messages = []

    def produce(self, topic, key, value, on_delivery=None):
        self.messages.append((topic, key, json.loads(value)))

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        return 0


def match(trade_id, **overrides):
    m = {"type": "match", "trade_id": trade_id, "product_id": "BTC-USD", "price": "100",
         "size": "1", "side": "sell", "time": "2026-11-15T22:00:00.000000Z"}
    return json.dumps(m | overrides)


def run_against(rejections, frames, timeout=10):
    """Fake exchange: rejects the first handshakes with the given (status,
    headers), then accepts, sends `frames`, and closes."""
    attempts = []
    producer = FakeProducer()
    ingestor = None

    def process_request(connection, request):
        attempts.append(request.path)
        if len(attempts) <= len(rejections):
            status, headers = rejections[len(attempts) - 1]
            resp = connection.respond(status, "unavailable\n")
            resp.headers.update(Headers(headers))
            return resp
        return None

    async def handler(ws):
        await ws.recv()  # subscribe
        for f in frames:
            await ws.send(f)
        await asyncio.sleep(0.2)
        ingestor.stop()

    async def main():
        nonlocal ingestor
        async with serve(handler, "127.0.0.1", 0, process_request=process_request) as server:
            port = server.sockets[0].getsockname()[1]
            ingestor = CoinbaseIngestor(["BTC-USD"], producer, ws_url=f"ws://127.0.0.1:{port}",
                                        initial_backoff=0.01, max_backoff=0.05)
            await asyncio.wait_for(ingestor.run(), timeout)

    asyncio.run(main())
    return ingestor, producer, attempts


def trades(producer):
    return [v["trade_id"] for t, _, v in producer.messages if t == "trades.raw"]


def test_reconnects_after_handshake_rejections():
    ingestor, producer, attempts = run_against(
        rejections=[(HTTPStatus.SERVICE_UNAVAILABLE, {}), (HTTPStatus.TOO_MANY_REQUESTS, {})],
        frames=[match(1), match(2)],
    )
    assert len(attempts) == 3
    assert ingestor.stats.reconnects == 2
    assert trades(producer) == [1, 2]


def test_honors_retry_after_on_429():
    start = time.monotonic()
    ingestor, producer, _ = run_against(
        rejections=[(HTTPStatus.TOO_MANY_REQUESTS, {"Retry-After": "1"})],
        frames=[match(1)],
    )
    assert time.monotonic() - start >= 1.0  # waited for Retry-After, not the 0.01s backoff
    assert trades(producer) == [1]


def test_malformed_messages_are_skipped_not_fatal():
    no_price = json.loads(match(2))
    del no_price["price"]
    ingestor, producer, _ = run_against(
        rejections=[],
        frames=[match(1), "not json", "[]", json.dumps(no_price),
                json.dumps({"type": "match", "product_id": "BTC-USD"}), match(3)],
    )
    assert trades(producer) == [1, 3]
    assert ingestor.stats.malformed == 4
    # The skipped trade surfaces as a gap, so the backfiller can recover it.
    gaps = [v for t, _, v in producer.messages if t == "ingest.gaps"]
    assert [(g["first_missing"], g["last_missing"]) for g in gaps] == [(2, 2)]
