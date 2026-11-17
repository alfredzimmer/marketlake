"""Dagster definitions: reference data, dbt, data quality checks, maintenance.

The streaming job (ingest -> silver/gold) runs continuously outside Dagster;
its tables are declared as external assets so dbt lineage and checks attach
to them.

Run: uv run dagster dev -m marketlake.orchestration.definitions
"""

from datetime import UTC, datetime, timedelta

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
from pyspark.sql import SparkSession

from marketlake.ref.coinbase_products import fetch, load
from marketlake.spark import maintenance
from marketlake.transform import DBT_DIR, write_profiles

# --- resources ---------------------------------------------------------------


class SparkResource(dg.ConfigurableResource):
    """SparkSession on the `lake` catalog (one per run process)."""

    app_name: str = "marketlake-dagster"

    def session(self) -> SparkSession:
        from marketlake.spark.session import build

        return build(self.app_name)


# --- streaming tables (produced outside Dagster) ----------------------------

silver_trades = dg.AssetSpec(
    key=["silver", "trades"], group_name="streaming",
    description="Deduplicated, typed trades written continuously by marketlake.spark.stream_trades.",
)
gold_bars_1m = dg.AssetSpec(
    key=["gold", "bars_1m"], group_name="streaming", deps=[silver_trades],
    description="1-minute bars emitted by the stream after the watermark closes each window.",
)

# --- reference data ------------------------------------------------------------


@dg.asset(key=["ref", "coinbase_products"], group_name="reference",
          description="Current Coinbase product list (full refresh).")
def coinbase_products(context: dg.AssetExecutionContext, spark: SparkResource) -> dg.MaterializeResult:
    n = load(spark.session(), fetch())
    context.log.info("loaded %d products", n)
    return dg.MaterializeResult(metadata={"products": n})


# --- dbt -------------------------------------------------------------------------

write_profiles()  # dbt CLI subprocesses read the generated profile
dbt_project = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR / ".profiles")
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# --- data quality checks on the streaming tables ------------------------------

FRESHNESS_WARN = timedelta(minutes=10)
MAX_REJECT_RATE = 0.01


@dg.asset_check(asset=silver_trades.key, description="Newest ingested trade is recent.")
def silver_trades_fresh(spark: SparkResource) -> dg.AssetCheckResult:
    newest = spark.session().sql(
        "SELECT cast(max(ingest_ts) AS string) FROM silver.trades").first()[0]
    if newest is None:
        return dg.AssetCheckResult(passed=False, metadata={"reason": "silver.trades is empty"})
    lag = datetime.now(UTC) - datetime.fromisoformat(newest).replace(tzinfo=UTC)
    return dg.AssetCheckResult(
        passed=lag <= FRESHNESS_WARN,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={"newest_ingest_ts": newest, "lag_minutes": round(lag.total_seconds() / 60, 1)},
    )


@dg.asset_check(asset=silver_trades.key,
                description="Share of trades.raw records quarantined in the last 24h stays under 1%.")
def silver_reject_rate(spark: SparkResource) -> dg.AssetCheckResult:
    s = spark.session()
    since = (datetime.now(UTC) - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    accepted = s.sql(f"SELECT count(*) FROM silver.trades WHERE ingest_ts >= TIMESTAMP '{since}'").first()[0]
    rejected = s.sql(f"SELECT count(*) FROM quarantine.trades WHERE quarantined_at >= TIMESTAMP '{since}'").first()[0]
    total = accepted + rejected
    rate = rejected / total if total else 0.0
    return dg.AssetCheckResult(
        passed=rate <= MAX_REJECT_RATE,
        metadata={"accepted": accepted, "rejected": rejected, "reject_rate": round(rate, 6)},
    )


# --- table maintenance ---------------------------------------------------------


@dg.op(config_schema={"steps": [str]})
def maintain_tables(context: dg.OpExecutionContext, spark: SparkResource) -> None:
    steps = {"compact": maintenance.compact, "expire": maintenance.expire,
             "orphans": maintenance.remove_orphans}
    s = spark.session()
    for table in maintenance.TABLES:
        before = maintenance.file_stats(s, table)
        for step in context.op_config["steps"]:
            steps[step](s, table)
        after = maintenance.file_stats(s, table)
        context.log.info(f"{table}: {before} -> {after}")


@dg.job(config={"ops": {"maintain_tables": {"config": {"steps": ["compact", "expire"]}}}})
def compact_and_expire():
    maintain_tables()


@dg.job(config={"ops": {"maintain_tables": {"config": {"steps": ["orphans"]}}}})
def remove_orphans():
    maintain_tables()


# --- schedules -----------------------------------------------------------------

refresh_reference = dg.define_asset_job("refresh_reference", selection=[coinbase_products])
build_dbt = dg.define_asset_job("build_dbt", selection=dg.AssetSelection.assets(dbt_models))
check_streaming = dg.define_asset_job(
    "check_streaming", selection=dg.AssetSelection.checks_for_assets(silver_trades.key))

defs = dg.Definitions(
    assets=[silver_trades, gold_bars_1m, coinbase_products, dbt_models],
    asset_checks=[silver_trades_fresh, silver_reject_rate],
    jobs=[refresh_reference, build_dbt, check_streaming, compact_and_expire, remove_orphans],
    schedules=[
        dg.ScheduleDefinition(job=refresh_reference, cron_schedule="15 0 * * *"),
        dg.ScheduleDefinition(job=build_dbt, cron_schedule="*/30 * * * *"),
        dg.ScheduleDefinition(job=check_streaming, cron_schedule="*/10 * * * *"),
        dg.ScheduleDefinition(job=compact_and_expire, cron_schedule="5 * * * *"),
        dg.ScheduleDefinition(job=remove_orphans, cron_schedule="30 3 * * *"),
    ],
    resources={
        "spark": SparkResource(),
        "dbt": DbtCliResource(project_dir=dbt_project),
    },
)
