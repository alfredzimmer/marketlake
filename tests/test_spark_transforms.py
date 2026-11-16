import json
from datetime import UTC, datetime
from decimal import Decimal

from marketlake.spark.stream_trades import bars_1m, parse_trades


def kafka_rows(spark, trades, extra_values=()):
    values = [json.dumps(t).encode() for t in trades] + list(extra_values)
    kafka_ts = datetime(2026, 11, 15, 22, 0, tzinfo=UTC)
    return spark.createDataFrame(
        [(v, 0, i, kafka_ts) for i, v in enumerate(values)],
        "value binary, partition int, offset long, timestamp timestamp",
    )


def trade(trade_id, price, size, side, ts, symbol="BTC-USD"):
    return {"exchange": "coinbase", "symbol": symbol, "trade_id": trade_id, "price": price,
            "size": size, "taker_side": side, "event_ts": ts, "ingest_ts": ts}


def test_parse_drops_malformed_records(spark):
    df = kafka_rows(spark, [trade(1, "100.5", "2", "buy", "2026-11-15T22:00:01.5Z")],
                    extra_values=[b"not json", json.dumps(trade(2, "abc", "1", "buy", "2026-11-15T22:00:02Z")).encode()])
    rows = parse_trades(df).collect()
    assert [r.trade_id for r in rows] == [1]
    assert rows[0].price == Decimal("100.5")


def test_bars_ohlcv_vwap_and_dedup(spark):
    trades = [
        trade(3, "102", "1", "sell", "2026-11-15T22:00:30Z"),
        trade(1, "100", "1", "buy", "2026-11-15T22:00:10Z"),
        trade(2, "105", "2", "buy", "2026-11-15T22:00:20Z"),
        trade(2, "105", "2", "buy", "2026-11-15T22:00:20Z"),  # duplicate delivery
        trade(4, "99", "1", "buy", "2026-11-15T22:01:05Z"),
    ]
    bars = {r.window_start.minute: r for r in bars_1m(parse_trades(kafka_rows(spark, trades)), "30 seconds").collect()}

    b = bars[0]
    assert (b.open, b.high, b.low, b.close) == (Decimal(100), Decimal(105), Decimal(100), Decimal(102))
    assert b.trade_count == 3
    assert b.volume == Decimal(4)
    assert b.vwap == Decimal(100 + 210 + 102) / 4
    assert (b.buy_volume, b.sell_volume) == (Decimal(3), Decimal(1))
    assert bars[1].trade_count == 1


def test_open_close_follow_trade_id_not_arrival_order(spark):
    trades = [
        trade(11, "50", "1", "buy", "2026-11-15T22:00:59Z"),
        trade(10, "40", "1", "buy", "2026-11-15T22:00:01Z"),
    ]
    (b,) = bars_1m(parse_trades(kafka_rows(spark, trades)), "30 seconds").collect()
    assert (b.open, b.close) == (Decimal(40), Decimal(50))
