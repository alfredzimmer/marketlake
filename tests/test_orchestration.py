from datetime import UTC, datetime, timedelta

import dagster as dg
import pytest
from pyspark.sql import SparkSession

from marketlake.spark.tables import create_all
from marketlake.transform import run as dbt_run


@pytest.fixture(scope="module")
def orch(spark):
    # The dbt assets are built from the manifest; CI has none until dbt parses.
    assert dbt_run(["parse"], conf={}).success
    from marketlake.orchestration import definitions

    class TestSpark(definitions.SparkResource):
        def session(self) -> SparkSession:
            return SparkSession.builder.getOrCreate()  # the pytest session

    create_all(spark)
    return definitions, TestSpark()


def test_definitions_load_with_lineage_and_schedules(orch):
    definitions, _ = orch
    repo = definitions.defs.get_repository_def()
    g = repo.asset_graph

    def parents(key):
        return {"/".join(k.path) for k in g.get(dg.AssetKey(key.split("/"))).parent_keys}

    assert parents("marts/fct_bars_1d") == {"silver/trades"}
    assert parents("marts/rpt_bar_reconciliation") == {"silver/trades", "gold/bars_1m"}
    assert parents("snapshots/snap_coinbase_products") == {"ref/coinbase_products"}
    checks = {c.name for c in g.asset_check_keys}
    assert {"silver_trades_fresh", "silver_reject_rate", "assert_stream_matches_batch"} <= checks
    assert {s.job_name for s in repo.schedule_defs} == {
        "refresh_reference", "build_dbt", "check_streaming", "compact_and_expire", "remove_orphans"}


def test_coinbase_products_asset_loads_ref_table(orch, spark, monkeypatch):
    definitions, test_spark = orch
    monkeypatch.setattr(definitions, "fetch", lambda: [
        {"id": "BTC-USD", "status": "online", "trading_disabled": False},
        {"id": "DOGE-USD", "status": "delisted", "trading_disabled": True}])
    result = dg.materialize([definitions.coinbase_products], resources={"spark": test_spark})
    assert result.success
    (event,) = result.get_asset_materialization_events()
    mat = event.materialization
    assert mat.asset_key == dg.AssetKey(["ref", "coinbase_products"])
    assert mat.metadata["products"].value == 2
    assert {r.id for r in spark.table("ref.coinbase_products").collect()} == {"BTC-USD", "DOGE-USD"}


def _ts(delta):
    return (datetime.now(UTC) - delta).strftime("%Y-%m-%dT%H:%M:%SZ")


def _seed(spark, ingest_age, n_trades, n_rejects):
    spark.sql("DELETE FROM silver.trades")
    spark.sql("DELETE FROM quarantine.trades")
    if n_trades:
        spark.sql(f"""INSERT INTO silver.trades
            SELECT 'coinbase', 'BTC-USD', id, 1, 1, 'buy', TIMESTAMP '{_ts(ingest_age)}',
                   TIMESTAMP '{_ts(ingest_age)}', 0, id FROM range({n_trades})""")
    if n_rejects:
        spark.sql(f"""INSERT INTO quarantine.trades
            SELECT 0, 100000 + id, current_timestamp(), cast('x' AS BINARY), 'invalid json',
                   current_timestamp() FROM range({n_rejects})""")


def _check(definitions, test_spark, name):
    ctx = dg.build_asset_check_context(resources={"spark": test_spark})
    return getattr(definitions, name)(ctx)


@pytest.mark.parametrize("age, passed", [(timedelta(minutes=2), True), (timedelta(hours=2), False)])
def test_freshness_check(orch, spark, age, passed):
    definitions, test_spark = orch
    _seed(spark, age, n_trades=10, n_rejects=0)
    assert _check(definitions, test_spark, "silver_trades_fresh").passed is passed


@pytest.mark.parametrize("rejects, passed", [(0, True), (1, True), (5, False)])
def test_reject_rate_check(orch, spark, rejects, passed):
    definitions, test_spark = orch
    _seed(spark, timedelta(minutes=1), n_trades=200, n_rejects=rejects)  # 5/205 = 2.4%
    assert _check(definitions, test_spark, "silver_reject_rate").passed is passed


def test_maintenance_job_runs(orch, spark):
    definitions, test_spark = orch
    _seed(spark, timedelta(minutes=1), n_trades=10, n_rejects=1)
    result = definitions.compact_and_expire.execute_in_process(resources={"spark": test_spark})
    assert result.success


def test_spark_progress_bar_disabled_for_dbt_event_stream():
    # dagster-dbt parses dbt's stdout as JSON events; Spark's progress bar on
    # the same stream corrupted events and dropped materializations.
    from marketlake.spark.session import spark_conf

    assert spark_conf()["spark.ui.showConsoleProgress"] == "false"
