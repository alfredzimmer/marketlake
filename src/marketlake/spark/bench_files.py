"""Scan benchmark over an Iceberg table with many small data files.

Reproduces #18: from the host, repeated scans of a few thousand files fail with
S3 'Connection reset' in Docker Desktop's port proxy. Inside the compose network
they don't.

Host:     uv run python -m marketlake.spark.bench_files create --files 6000
          uv run python -m marketlake.spark.bench_files scan --repeats 10
Network:  docker compose run --rm spark python -m marketlake.spark.bench_files scan
"""

from __future__ import annotations

import argparse
import time

from marketlake.spark.session import build

TABLE = "bench.many_files"


def create(spark, files: int, per_commit: int = 1000) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bench")
    spark.sql(f"DROP TABLE IF EXISTS {TABLE} PURGE")
    spark.sql(f"CREATE TABLE {TABLE} (id BIGINT, pad STRING) USING iceberg")
    for start in range(0, files, per_commit):
        n = min(per_commit, files - start)
        # One row per input partition -> one data file each.
        (spark.range(start, start + n, 1, n).selectExpr("id", "repeat('x', 200) AS pad")
         .writeTo(TABLE).append())
    print(f"{TABLE}: {spark.table(f'{TABLE}.files').count()} data files")


def scan(spark, repeats: int) -> int:
    failures = 0
    for i in range(repeats):
        t0 = time.monotonic()
        try:
            rows = spark.sql(f"SELECT count(*), sum(length(pad)) FROM {TABLE}").first()[0]
            print(f"scan {i}: ok rows={rows} {time.monotonic() - t0:.1f}s", flush=True)
        except Exception as e:
            failures += 1
            print(f"scan {i}: FAILED {time.monotonic() - t0:.1f}s {str(e).splitlines()[0][:200]}",
                  flush=True)
    print(f"{repeats - failures}/{repeats} scans ok")
    return failures


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["create", "scan"])
    ap.add_argument("--files", type=int, default=6000)
    ap.add_argument("--repeats", type=int, default=10)
    args = ap.parse_args()
    spark = build("marketlake-bench-files")
    if args.action == "create":
        create(spark, args.files)
    else:
        raise SystemExit(1 if scan(spark, args.repeats) else 0)


if __name__ == "__main__":
    main()
