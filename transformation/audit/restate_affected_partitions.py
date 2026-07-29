"""
restate_affected_partitions.py — Idempotent restatement from impact manifest.

For knowledge_date:
  1. Load impact_manifest (pending changes)
  2. Skip change_ids already in applied_change_ledger (status=APPLIED)
  3. For each affected (year,month,day) partition:
       - Snapshot BEFORE daily fraud report
       - Update staging.transactions.is_fraud from new_is_fraud
       - Snapshot AFTER
       - Write applied_change_ledger rows
  4. Empty pending set → no-op (safe to re-run)

Usage:
  spark-submit ... restate_affected_partitions.py YYYY-MM-DD
"""

from __future__ import annotations

import os
import sys
from datetime import date, datetime
from pathlib import Path

import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import structlog

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parent.parent.parent))
logger = structlog.get_logger(__name__)


def _load_cfg() -> dict:
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        return yaml.safe_load(f)


def _read_optional(spark: SparkSession, path: str) -> DataFrame | None:
    try:
        df = spark.read.parquet(path)
    except Exception:
        return None
    return df if df.head(1) else None


def _fraud_report_snapshot(
    txn: DataFrame,
    report_date: date,
    knowledge_date: date,
    snapshot_type: str,
    change_id: str,
    batch_id: str,
) -> DataFrame:
    day = txn.filter(F.to_date("transaction_date") == F.lit(report_date))
    agg = day.agg(
        F.count("*").alias("total_txn_count"),
        F.sum(F.col("amount").cast("double")).alias("total_amount_sum"),
        F.sum(F.when(F.col("is_fraud") == True, 1).otherwise(0)).alias("fraud_txn_count"),
        F.sum(F.when(F.col("is_fraud") == True, F.col("amount").cast("double")).otherwise(0.0))
         .alias("fraud_amount_sum"),
    )
    return agg.select(
        F.lit(report_date).alias("report_date"),
        F.lit(knowledge_date).alias("knowledge_date"),
        F.lit(snapshot_type).alias("snapshot_type"),
        F.lit(change_id).alias("change_id"),
        F.col("fraud_txn_count").cast("long"),
        F.col("fraud_amount_sum").cast("double"),
        F.col("total_txn_count").cast("long"),
        F.col("total_amount_sum").cast("double"),
        F.current_timestamp().alias("_snapshotted_at"),
        F.lit(batch_id).alias("_batch_id"),
        F.lit(report_date.year).alias("report_year"),
        F.lit(report_date.month).alias("report_month"),
        F.lit(report_date.day).alias("report_day"),
    )


def restate(spark: SparkSession, knowledge_date: date) -> dict:
    cfg = _load_cfg()
    staging_path = cfg["tables"]["transactions"]["staging"]
    manifest_path = cfg["tables"]["fraud_label_impact_manifest"]["warehouse"]
    ledger_path = cfg["tables"]["applied_change_ledger"]["warehouse"]
    snap_path = cfg["tables"]["daily_fraud_report_snapshots"]["warehouse"]
    index_path = cfg["tables"]["transaction_index"]["warehouse"]
    batch_id = f"restate_{knowledge_date.isoformat()}_{datetime.utcnow().strftime('%H%M%S')}"

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.shuffle.partitions", "20")

    ky, km, kd = knowledge_date.year, knowledge_date.month, knowledge_date.day
    manifest_part = (
        f"{manifest_path}/knowledge_year={ky}/knowledge_month={km}/knowledge_day={kd}"
    )

    try:
        pending = spark.read.parquet(manifest_part)
    except Exception:
        logger.info("restate.no_manifest", knowledge_date=knowledge_date.isoformat())
        return {"mode": "restate", "knowledge_date": knowledge_date.isoformat(),
                "pending": 0, "applied": 0, "partitions": 0, "batch_id": batch_id}

    # Drop partition cols if present as data columns from path read without basePath
    for c in ("knowledge_year", "knowledge_month", "knowledge_day"):
        if c in pending.columns:
            pending = pending.drop(c)

    applied = _read_optional(spark, ledger_path)
    if applied is not None:
        applied_ids = applied.filter(F.col("status") == "APPLIED").select("change_id").distinct()
        pending = pending.join(applied_ids, on="change_id", how="left_anti")

    pending_count = pending.count()
    if pending_count == 0:
        logger.info("restate.nothing_pending", knowledge_date=knowledge_date.isoformat())
        return {"mode": "restate", "knowledge_date": knowledge_date.isoformat(),
                "pending": 0, "applied": 0, "partitions": 0, "batch_id": batch_id}

    partitions = (
        pending.select("year", "month", "day").distinct()
        .orderBy("year", "month", "day")
        .collect()
    )

    applied_total = 0
    for row in partitions:
        y, m, d = int(row["year"]), int(row["month"]), int(row["day"])
        part_path = f"{staging_path}/year={y}/month={m}/day={d}"
        part_changes = pending.filter(
            (F.col("year") == y) & (F.col("month") == m) & (F.col("day") == d)
        ).select("change_id", "transaction_id", "event_date", "new_is_fraud")

        txn = spark.read.option("basePath", staging_path).parquet(part_path)
        part_change_id = f"{knowledge_date.isoformat()}_y{y}m{m}d{d}"

        before = _fraud_report_snapshot(
            txn, date(y, m, d), knowledge_date, "AS_PREVIOUSLY_REPORTED",
            part_change_id, batch_id,
        )
        before.write.mode("append").option("compression", "snappy") \
            .partitionBy("report_year", "report_month", "report_day") \
            .parquet(snap_path)

        updates = part_changes.select(
            F.col("transaction_id"),
            F.col("new_is_fraud").alias("_new_is_fraud"),
        )
        restated = txn.join(updates, on="transaction_id", how="left") \
            .withColumn(
                "is_fraud",
                F.when(F.col("_new_is_fraud").isNotNull(), F.col("_new_is_fraud"))
                 .otherwise(F.col("is_fraud"))
            ).drop("_new_is_fraud")

        after = _fraud_report_snapshot(
            restated, date(y, m, d), knowledge_date, "RESTATED",
            part_change_id, batch_id,
        )
        after.write.mode("append").option("compression", "snappy") \
            .partitionBy("report_year", "report_month", "report_day") \
            .parquet(snap_path)

        restated.repartition("year", "month", "day") \
            .write.mode("overwrite") \
            .option("compression", "snappy") \
            .partitionBy("year", "month", "day") \
            .parquet(staging_path)

        # Refresh transaction_index is_fraud for this partition
        try:
            idx = spark.read.option("basePath", index_path).parquet(
                f"{index_path}/year={y}/month={m}/day={d}"
            )
            if "is_fraud" in idx.columns:
                idx = idx.drop("is_fraud")
            idx2 = idx.join(
                restated.select("transaction_id", "is_fraud"),
                on="transaction_id", how="left",
            )
            idx2.repartition("year", "month", "day") \
                .write.mode("overwrite") \
                .option("compression", "snappy") \
                .partitionBy("year", "month", "day") \
                .parquet(index_path)
        except Exception as e:
            logger.warning("restate.index_refresh_skip", error=str(e))

        ledger = part_changes.select(
            "change_id", "transaction_id", "event_date",
            F.lit(knowledge_date).alias("knowledge_date"),
            F.current_timestamp().alias("applied_at"),
            F.lit("APPLIED").alias("status"),
            F.lit(f"year={y}/month={m}/day={d}").alias("partitions_touched"),
            F.lit(batch_id).alias("_batch_id"),
            F.lit(knowledge_date.year).alias("applied_year"),
            F.lit(knowledge_date.month).alias("applied_month"),
            F.lit(knowledge_date.day).alias("applied_day"),
        )
        n = ledger.count()
        ledger.write.mode("append").option("compression", "snappy") \
            .partitionBy("applied_year", "applied_month", "applied_day") \
            .parquet(ledger_path)
        applied_total += n
        logger.info("restate.partition_done", year=y, month=m, day=d, changes=n)

    return {
        "mode": "restate",
        "knowledge_date": knowledge_date.isoformat(),
        "pending": pending_count,
        "applied": applied_total,
        "partitions": len(partitions),
        "batch_id": batch_id,
    }


if __name__ == "__main__":
    from ingestion.spark_session import get_spark_session, stop_spark_session

    if len(sys.argv) < 2:
        print("Usage: restate_affected_partitions.py YYYY-MM-DD")
        sys.exit(1)

    spark = get_spark_session("restate_affected_partitions")
    try:
        result = restate(spark, date.fromisoformat(sys.argv[1]))
        print(f"\n=== DONE: {result} ===")
    finally:
        stop_spark_session(spark)
