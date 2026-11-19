"""REST backfill of trade-id gaps: paging, retries, and commit-after-ack."""

import json

import httpx
import pytest
from confluent_kafka import KafkaException

from marketlake.ingest.backfill import Backfiller, TradesClient
from marketlake.models import Gap

HISTORY = range(1, 5001)  # trade ids the fake exchange has


def rest_trade(i):
    return {"trade_id": i, "side": "sell", "size": "0.1", "price": f"{100 + i}.5",
            "time": "2026-11-17T16:05:00.000000Z"}


def fake_exchange(have=HISTORY, page_size=1000, fail_first=0):
    """Coinbase's /trades semantics: `after=N` -> ids below N, newest first."""
    calls = []

    def handler(request):
        calls.append(dict(request.url.params))
        if len(calls) <= fail_first:
            return httpx.Response(429, headers={"Retry-After": "0"})
        after = int(request.url.params["after"])
        limit = min(int(request.url.params["limit"]), page_size)
        ids = sorted((i for i in have if i < after), reverse=True)[:limit]
        return httpx.Response(200, json=[rest_trade(i) for i in ids])

    client = TradesClient(httpx.Client(transport=httpx.MockTransport(handler)),
                          min_interval_s=0, sleep=lambda s: None)
    return client, calls


class FakeProducer:
    def __init__(self, fail_topic=None):
        self.messages = []
        self.fail_topic = fail_topic
        self._pending = []

    def produce(self, topic, key, value, on_delivery=None):
        self.messages.append((topic, key, json.loads(value)))
        self._pending.append((topic, on_delivery))

    def poll(self, timeout):
        for topic, cb in self._pending:
            cb("broker down" if topic == self.fail_topic else None, None)
        self._pending.clear()
        return 0

    def flush(self, timeout):
        return self.poll(timeout)

    def trades(self):
        return [v for t, _, v in self.messages if t == "trades.raw"]

    def results(self):
        return [v for t, _, v in self.messages if t == "ingest.backfills"]


def gap(first, last, symbol="BTC-USD"):
    return Gap(exchange="coinbase", symbol=symbol, first_missing=first, last_missing=last,
               detected_ts="2026-11-17T16:06:00+00:00")


def test_pages_back_through_a_gap_larger_than_one_page():
    client, calls = fake_exchange()
    producer = FakeProducer()
    result = Backfiller(producer, client).backfill(gap(1200, 3700))
    ids = [t["trade_id"] for t in producer.trades()]
    assert sorted(ids) == list(range(1200, 3701))
    assert len(ids) == len(set(ids))
    # 3701 -> 2701 -> 1701 -> 701: stops once a page passes the gap's start.
    assert [int(c["after"]) for c in calls] == [3701, 2701, 1701]
    assert result.status == "complete" and result.fetched == 2501
    assert producer.results() == [json.loads(result.to_json())]


def test_trades_match_the_websocket_shape():
    client, _ = fake_exchange()
    producer = FakeProducer()
    Backfiller(producer, client).backfill(gap(10, 10))
    [(topic, key, t)] = [m for m in producer.messages if m[0] == "trades.raw"]
    assert key == "coinbase:BTC-USD"
    # REST reports the maker side, like the websocket; we store the taker.
    assert t | {"ingest_ts": None} == {
        "exchange": "coinbase", "symbol": "BTC-USD", "trade_id": 10, "price": "110.5",
        "size": "0.1", "taker_side": "buy", "event_ts": "2026-11-17T16:05:00.000000Z",
        "ingest_ts": None}


def test_ids_the_exchange_lacks_are_reported_not_retried():
    client, _ = fake_exchange(have=[i for i in HISTORY if i not in (51, 52)])
    producer = FakeProducer()
    result = Backfiller(producer, client).backfill(gap(50, 60))
    assert (result.status, result.fetched, result.unavailable) == ("partial", 9, 2)


def test_rate_limits_are_retried():
    client, calls = fake_exchange(fail_first=3)
    producer = FakeProducer()
    assert Backfiller(producer, client).backfill(gap(1, 5)).status == "complete"
    assert len(calls) == 4


def test_absurd_gaps_are_skipped_without_fetching():
    client, calls = fake_exchange()
    producer = FakeProducer()
    result = Backfiller(producer, client, max_gap_ids=100).backfill(gap(1, 10_000))
    assert result.status == "skipped" and calls == [] and producer.trades() == []


class FakeMessage:
    def __init__(self, value, offset):
        self._value, self._offset = value, offset

    def value(self): return self._value
    def error(self): return None
    def topic(self): return "ingest.gaps"
    def partition(self): return 0
    def offset(self): return self._offset


class FakeConsumer:
    def __init__(self, values):
        self.queue = [FakeMessage(v, i) for i, v in enumerate(values)]
        self.committed = []

    def subscribe(self, topics): pass

    def poll(self, timeout):
        return self.queue.pop(0) if self.queue else None

    def commit(self, message, asynchronous):
        assert asynchronous is False
        self.committed.append(message.offset())


def test_gap_offset_is_committed_only_after_trades_are_acked():
    client, _ = fake_exchange()
    consumer = FakeConsumer([gap(1, 5).to_json()])
    with pytest.raises(KafkaException):
        Backfiller(FakeProducer(fail_topic="trades.raw"), client).run(
            consumer, stop=lambda: False)
    assert consumer.committed == []


def test_run_commits_each_gap_and_skips_malformed_events():
    client, _ = fake_exchange()
    consumer = FakeConsumer([b"not json", gap(1, 3).to_json(), gap(7, 8, "ETH-USD").to_json()])
    producer = FakeProducer()
    Backfiller(producer, client).run(consumer, stop=lambda: not consumer.queue)
    assert consumer.committed == [0, 1, 2]
    assert [(t["symbol"], t["trade_id"]) for t in producer.trades()] == [
        ("BTC-USD", 3), ("BTC-USD", 2), ("BTC-USD", 1), ("ETH-USD", 8), ("ETH-USD", 7)]


def scripted_client(responses):
    """Serves the given responses in order, then repeats the last one."""
    calls = []

    def handler(request):
        calls.append(request)
        return responses[min(len(calls), len(responses)) - 1]

    return TradesClient(httpx.Client(transport=httpx.MockTransport(handler)),
                        min_interval_s=0, sleep=lambda s: None), calls


def test_permanent_errors_are_recorded_not_crash_looped():
    client, calls = scripted_client([httpx.Response(404, json={"message": "NotFound"})])
    producer = FakeProducer()
    consumer = FakeConsumer([gap(1, 5, "GONE-USD").to_json(), gap(1, 3).to_json()])
    Backfiller(producer, client).run(consumer, stop=lambda: not consumer.queue)
    # The 404 is not retried, and doesn't block the gap behind it.
    assert consumer.committed == [0, 1]
    assert [r["status"] for r in producer.results()] == ["failed", "failed"]
    assert len(calls) == 2


def test_non_json_200_is_retried():
    client, calls = scripted_client([
        httpx.Response(200, text="<html>cloudflare</html>"),
        httpx.Response(200, json=[rest_trade(2), rest_trade(1)]),
    ])
    producer = FakeProducer()
    assert Backfiller(producer, client).backfill(gap(1, 2)).status == "complete"
    assert len(calls) == 2


def test_gaps_from_other_exchanges_are_left_alone():
    client, calls = fake_exchange()
    producer = FakeProducer()
    other = Gap(exchange="binance", symbol="BTCUSDT", first_missing=1, last_missing=5,
                detected_ts="2026-11-17T16:06:00+00:00")
    consumer = FakeConsumer([other.to_json()])
    Backfiller(producer, client).run(consumer, stop=lambda: not consumer.queue)
    assert consumer.committed == [0] and calls == [] and producer.messages == []
