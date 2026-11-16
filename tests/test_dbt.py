"""dbt project against the test Spark session (in-process, local Iceberg catalog).

dbt-spark's `session` method calls SparkSession.builder.getOrCreate(), which
returns the pytest session, so these run the real models, snapshot and tests.
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from marketlake.ref.coinbase_products import load
from marketlake.spark.stream_trades import SILVER_COLUMNS, bars_1m, merge_silver, parse_trades
from marketlake.spark.tables import create_all
from marketlake.transform import run
from test_spark_transforms import kafka_rows, trade


def dbt(*args):
    result = run(list(args), conf={})  # reuse the running test session
    return result


def statuses(result):
    return {r.node.name: str(r.status) for r in result.result}


def product(pid, status="online", tick="0.01"):
    return {"id": pid, "base_currency": pid.split("-")[0], "quote_currency": "USD",
            "quote_increment": tick, "base_increment": "0.00000001", "min_market_funds": "1",
            "status": status, "status_message": "", "trading_disabled": status != "online",
            "post_only": False, "limit_only": False, "cancel_only": False,
            "auction_mode": False, "fx_stablecoin": False}


def add_trades(spark, trades):
    merge_silver(parse_trades(kafka_rows(spark, trades)).select(*SILVER_COLUMNS), 0)


@pytest.fixture(scope="module")
def lake(spark):
    create_all(spark)
    for t in ("silver.trades", "gold.bars_1m"):
        spark.sql(f"DELETE FROM {t}")
    for ns in ("marts", "snapshots"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")
        for (_, name, *_rest) in spark.sql(f"SHOW TABLES IN {ns}").collect():
            spark.sql(f"DROP TABLE IF EXISTS {ns}.{name} PURGE")

    trades = [
        trade(1, "100", "1", "buy", "2026-11-14T10:00:05Z"),
        trade(2, "110", "1", "sell", "2026-11-14T10:00:30Z"),
        trade(3, "105", "2", "buy", "2026-11-15T09:00:00Z"),
        trade(4, "95", "2", "sell", "2026-11-15T09:30:00Z"),
        trade(5, "101", "1", "buy", "2026-11-15T23:00:00Z"),
        trade(10, "50", "1", "buy", "2026-11-15T09:00:00Z", symbol="ETH-USD"),
    ]
    add_trades(spark, trades)
    # Gold as the stream would have written it (same bar definition).
    bars_1m(spark.table("silver.trades"), "30 seconds").writeTo("gold.bars_1m").append()
    load(spark, [product("BTC-USD"), product("ETH-USD")],
         fetched_at=datetime(2026, 11, 15, tzinfo=UTC))
    return spark


def test_build_passes_and_bars_are_correct(lake):
    result = dbt("build")
    assert result.success, statuses(result)

    d = {(r.symbol, str(r.trade_date)): r for r in lake.table("marts.fct_bars_1d").collect()}
    b = d[("BTC-USD", "2026-11-15")]
    assert (b.open, b.high, b.low, b.close) == (Decimal(105), Decimal(105), Decimal(95), Decimal(101))
    assert b.trade_count == 3 and b.volume == Decimal(5)
    assert b.vwap == Decimal(210 + 190 + 101) / 5
    assert b.missing_trade_ids == 0
    assert {r.status for r in lake.table("marts.rpt_bar_reconciliation").collect()} == {"match"}


def test_snapshot_tracks_changes_as_scd2(lake):
    load(lake, [product("BTC-USD", status="delisted", tick="0.1")],  # ETH-USD vanished
         fetched_at=datetime(2026, 11, 16, tzinfo=UTC))
    assert dbt("snapshot").success
    assert dbt("run", "--select", "dim_symbol").success

    rows = lake.sql("SELECT * FROM marts.dim_symbol ORDER BY symbol, valid_from").collect()
    btc = [r for r in rows if r.symbol == "BTC-USD"]
    assert [(r.status, r.tick_size, r.is_current) for r in btc] == [
        ("online", Decimal("0.01"), False), ("delisted", Decimal("0.1"), True)]
    assert btc[0].valid_to == btc[1].valid_from
    eth = [r for r in rows if r.symbol == "ETH-USD"]
    assert len(eth) == 1 and not eth[0].is_current  # hard delete closes the version
    assert dbt("test", "--select", "dim_symbol", "assert_one_current_version_per_symbol").success


def test_incremental_run_picks_up_late_trades_in_lookback(lake):
    add_trades(lake, [trade(6, "120", "1", "buy", "2026-11-14T12:00:00Z")])  # day already built
    assert dbt("run", "--select", "fct_bars_1d").success

    r = lake.sql("""SELECT * FROM marts.fct_bars_1d
                    WHERE symbol = 'BTC-USD' AND trade_date = DATE '2026-11-14'""").first()
    assert (r.trade_count, r.high, r.close) == (3, Decimal(120), Decimal(120))
    assert lake.table("marts.fct_bars_1d").count() == 3  # merged, not appended


def test_price_outlier_fails_the_build(lake):
    add_trades(lake, [trade(7, "1.00", "0.001", "buy", "2026-11-15T23:30:00Z")])
    assert dbt("run", "--select", "fct_bars_1d").success
    result = dbt("test", "--select", "assert_no_price_outliers")
    assert not result.success
    lake.sql("DELETE FROM silver.trades WHERE trade_id = 7")
