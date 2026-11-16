"""Run dbt against the lake catalog.

The dbt profile is generated from `spark_conf()` so dbt (CLI, Dagster, tests)
and the Spark jobs can't drift apart on catalog or S3 settings.

Run: uv run python -m marketlake.transform build      # any dbt command + args
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

DBT_DIR = Path(__file__).resolve().parents[2] / "dbt"
PROFILES_DIR = DBT_DIR / ".profiles"


def write_profiles(conf: dict[str, str] | None = None, profiles_dir: Path = PROFILES_DIR) -> Path:
    """Writes profiles.yml for dbt-spark's in-process `session` method. With
    conf=None the Spark settings come from `spark_conf()`; pass {} to reuse an
    already-running SparkSession as-is (tests)."""
    if conf is None:
        from marketlake.spark.session import spark_conf

        conf = spark_conf()
    profile = {"marketlake": {"target": "lake", "outputs": {"lake": {
        "type": "spark",
        "method": "session",
        "host": "NA",
        "schema": "marts",
        "server_side_parameters": conf,
    }}}}
    profiles_dir.mkdir(parents=True, exist_ok=True)
    (profiles_dir / "profiles.yml").write_text(yaml.safe_dump(profile, sort_keys=False))
    return profiles_dir


def run(args: list[str], conf: dict[str, str] | None = None):
    from dbt.cli.main import dbtRunner

    profiles_dir = write_profiles(conf)
    return dbtRunner().invoke(
        [*args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(profiles_dir)])


def main() -> None:
    result = run(sys.argv[1:] or ["build"])
    sys.exit(0 if result.success else 1)


if __name__ == "__main__":
    main()
