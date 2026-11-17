import json

from marketlake.ingest.coinbase import CoinbaseIngestor
from marketlake.ingest.spill import SpillLog


class Msg:
    def __init__(self, topic, key, value):
        self._t, self._k, self._v = topic, key, value

    def topic(self):
        return self._t

    def key(self):
        return self._k

    def value(self):
        return self._v


class FlakyProducer:
    """Delivers on poll/flush, or fails every queued record while `down`.
    With `queue_limit`, produce raises BufferError like librdkafka when full."""

    def __init__(self, queue_limit=None):
        self.down = False
        self.queue_limit = queue_limit
        self.queue = []
        self.delivered = []

    def produce(self, topic, key, value, on_delivery=None):
        if self.queue_limit is not None and len(self.queue) >= self.queue_limit:
            raise BufferError("queue full")
        k = key.encode() if isinstance(key, str) else key
        self.queue.append((Msg(topic, k, value), on_delivery))

    def poll(self, timeout=0):
        if self.down and self.queue_limit is not None:
            return 0  # stuck: nothing completes while the broker is down
        queue, self.queue = self.queue, []
        for msg, cb in queue:
            if self.down:
                cb and cb("broker down", msg)
            else:
                self.delivered.append(msg)
                cb and cb(None, msg)
        return len(queue)

    def flush(self, timeout=0):
        self.poll()
        return len(self.queue)

    def purge(self):
        queue, self.queue = self.queue, []
        for msg, cb in queue:
            cb and cb("purged", msg)


def match(trade_id):
    return json.dumps({"type": "match", "trade_id": trade_id, "product_id": "BTC-USD",
                       "price": "100", "size": "1", "side": "sell",
                       "time": "2026-11-15T22:00:00.000000Z"})


def delivered_ids(producer):
    return sorted(json.loads(m.value())["trade_id"] for m in producer.delivered
                  if m.topic() == "trades.raw")


def make(tmp_path, producer):
    return CoinbaseIngestor(["BTC-USD"], producer, spill_path=tmp_path / "spill.jsonl",
                            state_path=tmp_path / "trade_ids.json")


def test_outage_spills_then_replays_everything(tmp_path):
    p = FlakyProducer()
    ing = make(tmp_path, p)
    ing.handle(match(1))
    p.down = True
    for i in (2, 3, 4):
        ing.handle(match(i))
    assert ing.stats.spilled == 3 and delivered_ids(p) == [1]

    assert ing.replay_spill() is False  # still down: kept, not duplicated
    assert ing.replay_spill() is False
    assert len(ing.spill.take()) == 3

    p.down = False
    ing.handle(match(5))
    assert ing.replay_spill() is True
    assert delivered_ids(p) == [1, 2, 3, 4, 5]
    assert not ing.spill.pending()


def test_full_local_queue_spills_instead_of_raising(tmp_path):
    p = FlakyProducer(queue_limit=2)
    p.down = True
    ing = make(tmp_path, p)
    for i in range(1, 6):
        ing.handle(match(i))  # 3rd+ produce raises BufferError
    assert ing.stats.spilled == 3

    p.down, p.queue_limit = False, None
    p.poll()
    assert ing.replay_spill() is True
    assert delivered_ids(p) == [1, 2, 3, 4, 5]


def test_shutdown_spills_undelivered_and_next_run_replays(tmp_path):
    p = FlakyProducer(queue_limit=100)
    p.down = True
    ing = make(tmp_path, p)
    for i in (1, 2):
        ing.handle(match(i))
    # Same as run()'s shutdown path.
    assert p.flush(10) == 2
    p.purge()
    ing.spill.close()

    p2 = FlakyProducer()
    assert make(tmp_path, p2).replay_spill() is True
    assert delivered_ids(p2) == [1, 2]


def test_leftover_replay_batch_survives_crash(tmp_path):
    spill = SpillLog(tmp_path / "spill.jsonl")
    spill.append("trades.raw", b"k", b'{"trade_id": 1}')
    assert len(spill.take()) == 1  # crash here: .replay left on disk
    spill.append("trades.raw", b"k", b'{"trade_id": 2}')

    records = SpillLog(tmp_path / "spill.jsonl").take()
    assert [r[2] for r in records] == [b'{"trade_id": 1}', b'{"trade_id": 2}']


def test_replay_completes_under_continuous_live_traffic(tmp_path):
    class BusyProducer(FlakyProducer):
        def flush(self, timeout=0):
            self.poll()
            return 1  # live trades keep arriving, so the queue is never empty

    p = BusyProducer()
    ing = make(tmp_path, p)
    p.down = True
    ing.handle(match(1))
    p.down = False
    assert ing.replay_spill(flush_timeout=0.5) is True
    assert not ing.spill.pending()
    assert delivered_ids(p) == [1]
