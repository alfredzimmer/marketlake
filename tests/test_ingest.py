from marketlake.ingest.coinbase import parse_match
from marketlake.ingest.gaps import GapDetector


def test_contiguous_ids_have_no_gap():
    d = GapDetector("coinbase")
    assert d.observe("BTC-USD", 10) == (True, None)
    assert d.observe("BTC-USD", 11) == (True, None)


def test_gap_reports_missing_range():
    d = GapDetector("coinbase")
    d.observe("BTC-USD", 10)
    is_new, gap = d.observe("BTC-USD", 15)
    assert is_new
    assert (gap.first_missing, gap.last_missing, gap.size) == (11, 14, 4)


def test_duplicates_and_stale_ids_are_flagged():
    d = GapDetector("coinbase")
    d.observe("BTC-USD", 10)
    assert d.observe("BTC-USD", 10) == (False, None)
    assert d.observe("BTC-USD", 9) == (False, None)


def test_symbols_tracked_independently():
    d = GapDetector("coinbase")
    d.observe("BTC-USD", 100)
    d.observe("ETH-USD", 5)
    assert d.observe("BTC-USD", 101) == (True, None)
    assert d.observe("ETH-USD", 6) == (True, None)


def test_parse_match_flips_maker_side_to_taker():
    msg = {"type": "match", "trade_id": 7, "product_id": "BTC-USD", "price": "65000.01",
           "size": "0.5", "side": "sell", "time": "2026-11-15T22:00:00.000000Z"}
    t = parse_match(msg, "2026-11-15T22:00:00.100000+00:00")
    assert t.taker_side == "buy"
    assert t.price == "65000.01"
    assert t.key == "coinbase:BTC-USD"
