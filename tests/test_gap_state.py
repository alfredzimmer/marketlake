"""Gap detection across ingestor restarts (#8)."""

import json

from test_spill import FlakyProducer, make, match

from marketlake.ingest.gaps import GapDetector
from marketlake.ingest.state import TradeIdState


def gaps(producer):
    return [json.loads(m.value()) for m in producer.delivered if m.topic() == "ingest.gaps"]


def test_seeded_detector_reports_gap_on_first_trade():
    d = GapDetector("coinbase", {"BTC-USD": 10})
    is_new, gap = d.observe("BTC-USD", 15)
    assert is_new and (gap.first_missing, gap.last_missing) == (11, 14)
    assert d.observe("BTC-USD", 15) == (False, None)


def test_restart_reports_downtime_as_gap(tmp_path):
    p = FlakyProducer()
    ing = make(tmp_path, p)
    for i in (1, 2, 3):
        ing.handle(match(i))
    p.poll()
    ing.state.save()  # what run() does periodically and on shutdown

    # Ids 4..6 traded while the ingestor was down.
    p2 = FlakyProducer()
    ing2 = make(tmp_path, p2)
    ing2.handle(match(7))
    p2.poll()
    assert [(g["first_missing"], g["last_missing"]) for g in gaps(p2)] == [(4, 6)]
    assert ing2.stats.missing == 3


def test_trades_lost_in_a_crash_fall_inside_the_gap(tmp_path):
    p = FlakyProducer()
    ing = make(tmp_path, p)
    for i in (1, 2):
        ing.handle(match(i))
    p.poll()
    # 3 and 4 are seen and queued in the producer, then the process is
    # killed before they're delivered: not in Kafka, not in the spill.
    p.down, p.queue_limit = True, 100  # broker stalled: nothing completes
    for i in (3, 4):
        ing.handle(match(i))
    ing.state.save()

    p2 = FlakyProducer()
    ing2 = make(tmp_path, p2)
    ing2.handle(match(5))
    p2.poll()
    assert [(g["first_missing"], g["last_missing"]) for g in gaps(p2)] == [(3, 4)]


def test_spilled_trades_count_as_durable(tmp_path):
    p = FlakyProducer()
    ing = make(tmp_path, p)
    ing.handle(match(1))
    p.down = True
    ing.handle(match(2))
    p.poll()  # 2 fails delivery and goes to the spill journal
    ing.state.save()

    p2 = FlakyProducer()
    ing2 = make(tmp_path, p2)
    ing2.handle(match(3))  # 2 will be replayed from the spill: no gap
    p2.poll()
    assert gaps(p2) == []


def test_state_file_is_written_atomically(tmp_path):
    s = TradeIdState(tmp_path / "s.json")
    s.advance("BTC-USD", 5)
    s.advance("BTC-USD", 3)  # never moves backwards
    s.save()
    assert json.loads((tmp_path / "s.json").read_text()) == {"BTC-USD": 5}
    assert not (tmp_path / "s.json.tmp").exists()
    assert TradeIdState(tmp_path / "s.json").last() == {"BTC-USD": 5}
