"""
reconcile_batch.py — Source-to-staging reconciliation invariant.

  raw_count - duplicate_count = accepted_count + quarantined_count
  unexplained_difference must be 0

Usage:
  spark-submit ... reconcile_batch.py YYYY-MM-DD
"""

from __future__ import annotations

import os
import sys
from datetime import date, datetime
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import structlog

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parent.parent.parent))
logger = structlog.get_logger(__name__)


def _load_cfg() -> dict:
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        return yaml.safe_load(f)


def reconcile(spark: SparkSession, event_date: date, knowledge_date: date | None = None) -> dict:
    cfg = _load_cfg()
    y, m, d = event_date.year, event_date.month, event_date.day
    if knowledge_date is None:
        knowledge_date = event_date

    raw_path = f"{cfg['tables']['transactions']['raw']}/year={y}/month={m}/day={d}"
    staging_path = (
        f"{cfg['tables']['transactions']['staging']}/year={y}/month={m}/day={d}"
    )
    quarantine_path = (
        f"{cfg['tables']['transactions']['quarantine']}/year={y}/month={m}/day={d}"
    )
    out_path = cfg["tables"]["batch_reconciliation_results"]["warehouse"]
    batch_id = f"recon_{event_date.isoformat()}_{datetime.utcnow().strftime('%H%M%S')}"

    logger.info("reconcile.start", event_date=event_date.isoformat())

    raw = spark.read.parquet(raw_path)
    # Raw uses column `id` as transaction key
    id_col = "id" if "id" in raw.columns else "transaction_id"
    raw_count = raw.count()
    dup_count = (
        raw.groupBy(id_col).count()
        .filter(F.col("count") > 1)
        .agg(F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("dups"))
        .collect()[0]["dups"]
    )
    if dup_count is None:
        dup_count = 0
    else:
        dup_count = int(dup_count)

    try:
        staging = spark.read.parquet(staging_path)
        accepted_count = staging.filter(F.col("is_valid") == True).count()
    except Exception:
        accepted_count = 0

    try:
        quarantined_count = spark.read.parquet(quarantine_path).count()
    except Exception:
        quarantined_count = 0

    unexplained = int(raw_count) - int(dup_count) - int(accepted_count) - int(quarantined_count)
    invariant_ok = unexplained == 0

    result_df = spark.createDataFrame(
        [(
            event_date,
            knowledge_date,
            int(raw_count),
            int(dup_count),
            int(accepted_count),
            int(quarantined_count),
            int(unexplained),
            invariant_ok,
            datetime.utcnow(),
            batch_id,
            y, m, d,
        )],
        schema="""
            event_date date,
            knowledge_date date,
            raw_count long,
            duplicate_count long,
            accepted_count long,
            quarantined_count long,
            unexplained_difference long,
            invariant_ok boolean,
            _reconciled_at timestamp,
            _batch_id string,
            event_year int,
            event_month int,
            event_day int
        """,
    )

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    result_df.write.mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("event_year", "event_month", "event_day") \
        .parquet(out_path)

    out = {
        "event_date": event_date.isoformat(),
        "raw_count": int(raw_count),
        "duplicate_count": int(dup_count),
        "accepted_count": int(accepted_count),
        "quarantined_count": int(quarantined_count),
        "unexplained_difference": int(unexplained),
        "invariant_ok": invariant_ok,
        "batch_id": batch_id,
    }
    logger.info("reconcile.done", **out)
    return out


if __name__ == "__main__":
    from ingestion.spark_session import get_spark_session, stop_spark_session

    if len(sys.argv) < 2:
        print("Usage: reconcile_batch.py YYYY-MM-DD [knowledge_date]")
        sys.exit(1)

    event_date = date.fromisoformat(sys.argv[1])
    knowledge_date = date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else event_date

    spark = get_spark_session("reconcile_batch")
    try:
        result = reconcile(spark, event_date, knowledge_date)
        print(f"\n=== DONE: {result} ===")
    finally:
        stop_spark_session(spark)
