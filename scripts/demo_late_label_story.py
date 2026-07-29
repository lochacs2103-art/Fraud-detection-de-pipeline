"""
demo_late_label_story.py — Day1 → Day3 late-label demo (memory-light).

Does NOT rewrite 8.9M fraud_labels (that OOMs 2GB executors).
Instead:
  1. Pick one No/false victim from transaction_index
  2. Snapshot Day-1 report stats for its event partition
  3. Write a 1-row impact_manifest (No → Yes)
  4. Run restate on that knowledge_date (1 partition only)
  5. Snapshot Day-3 stats + reconcile
  6. Restore that one txn's is_fraud on the partition

Prints DEMO_METRICS for README.
"""

from __future__ import annotations

import os
import sys
from datetime import date, datetime
from pathlib import Path

import yaml
from pyspark.sql import Row
from pyspark.sql import functions as F
import structlog

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parent.parent))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from transformation.audit.restate_affected_partitions import restate
from transformation.audit.reconcile_batch import reconcile

logger = structlog.get_logger(__name__)
KNOWLEDGE_DAY3 = date(2020, 1, 3)


def _cfg():
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        return yaml.safe_load(f)


def _pick_victim(spark, cfg) -> dict:
    index_path = cfg["tables"]["transaction_index"]["warehouse"]
    # Prefer clearly non-fraud rows; fall back to any indexed row
    idx = spark.read.parquet(index_path).select(
        "transaction_id", "event_date", "year", "month", "day", "amount", "is_fraud"
    )
    victim_df = idx.filter(
        (F.col("is_fraud") == False) | F.col("is_fraud").isNull()
    ).limit(1)
    rows = victim_df.collect()
    if not rows:
        rows = idx.limit(1).collect()
    if not rows:
        raise RuntimeError("transaction_index is empty — run build_transaction_index --full first")
    r = rows[0]
    return {
        "transaction_id": r["transaction_id"],
        "event_date": r["event_date"].isoformat() if r["event_date"] else None,
        "year": int(r["year"]),
        "month": int(r["month"]),
        "day": int(r["day"]),
        "amount": float(r["amount"]) if r["amount"] is not None else 0.0,
        "is_fraud_before": r["is_fraud"],
    }


def _snapshot_stats(spark, cfg, y: int, m: int, d: int) -> dict:
    path = f"{cfg['tables']['transactions']['staging']}/year={y}/month={m}/day={d}"
    df = spark.read.parquet(path)
    row = df.agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("is_fraud") == True, 1).otherwise(0)).alias("fraud_n"),
        F.sum(
            F.when(F.col("is_fraud") == True, F.col("amount").cast("double")).otherwise(0.0)
        ).alias("fraud_amt"),
    ).collect()[0]
    return {
        "total_txn": int(row["total"]),
        "fraud_txn": int(row["fraud_n"] or 0),
        "fraud_amount": float(row["fraud_amt"] or 0.0),
    }


def _write_one_row_manifest(spark, cfg, victim: dict, knowledge_date: date) -> str:
    manifest_path = cfg["tables"]["fraud_label_impact_manifest"]["warehouse"]
    txn_id = victim["transaction_id"]
    event_date = date(victim["year"], victim["month"], victim["day"])
    change_id = f"{knowledge_date.isoformat()}_{txn_id}_LABEL_CHANGED"
    batch_id = f"demo_{knowledge_date.isoformat()}"

    row = Row(
        change_id=change_id,
        transaction_id=txn_id,
        event_date=event_date,
        knowledge_date=knowledge_date,
        old_is_fraud=False,
        new_is_fraud=True,
        year=victim["year"],
        month=victim["month"],
        day=victim["day"],
        change_type="LABEL_CHANGED",
        _detected_at=datetime.utcnow(),
        _batch_id=batch_id,
        knowledge_year=knowledge_date.year,
        knowledge_month=knowledge_date.month,
        knowledge_day=knowledge_date.day,
    )
    spark.createDataFrame([row]).repartition(1) \
        .write.mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("knowledge_year", "knowledge_month", "knowledge_day") \
        .parquet(manifest_path)
    logger.info("demo.manifest_written", change_id=change_id, transaction_id=txn_id)
    return change_id


def _restore_victim_label(spark, cfg, victim: dict, original_is_fraud) -> None:
    """Put staging + index is_fraud back for the single victim row."""
    y, m, d = victim["year"], victim["month"], victim["day"]
    staging_path = cfg["tables"]["transactions"]["staging"]
    index_path = cfg["tables"]["transaction_index"]["warehouse"]
    part = f"{staging_path}/year={y}/month={m}/day={d}"
    txn_id = victim["transaction_id"]

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df = spark.read.option("basePath", staging_path).parquet(part)
    restored = df.withColumn(
        "is_fraud",
        F.when(F.col("transaction_id") == txn_id, F.lit(original_is_fraud).cast("boolean"))
         .otherwise(F.col("is_fraud")),
    )
    restored.repartition("year", "month", "day") \
        .write.mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("year", "month", "day") \
        .parquet(staging_path)

    try:
        idx_part = f"{index_path}/year={y}/month={m}/day={d}"
        idx = spark.read.option("basePath", index_path).parquet(idx_part)
        idx2 = idx.withColumn(
            "is_fraud",
            F.when(F.col("transaction_id") == txn_id, F.lit(original_is_fraud).cast("boolean"))
             .otherwise(F.col("is_fraud")),
        )
        idx2.repartition("year", "month", "day") \
            .write.mode("overwrite") \
            .option("compression", "snappy") \
            .partitionBy("year", "month", "day") \
            .parquet(index_path)
    except Exception as e:
        logger.warning("demo.index_restore_skip", error=str(e))


def main(spark) -> dict:
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    cfg = _cfg()
    victim = _pick_victim(spark, cfg)
    txn_id = victim["transaction_id"]
    y, m, d = victim["year"], victim["month"], victim["day"]
    event_date = date(y, m, d)
    original = victim["is_fraud_before"]

    print("\n=== DEMO VICTIM ===")
    print(victim)

    before = _snapshot_stats(spark, cfg, y, m, d)
    print("\n=== DAY1 REPORT (before late label) ===")
    print(before)

    change_id = _write_one_row_manifest(spark, cfg, victim, KNOWLEDGE_DAY3)
    print("\n=== DAY3 IMPACT MANIFEST ===")
    print({"change_id": change_id, "old_is_fraud": False, "new_is_fraud": True})

    restate_day3 = restate(spark, KNOWLEDGE_DAY3)
    print("\n=== DAY3 RESTATE ===")
    print(restate_day3)

    after = _snapshot_stats(spark, cfg, y, m, d)
    print("\n=== DAY3 REPORT (after restatement) ===")
    print(after)

    recon = reconcile(spark, event_date, KNOWLEDGE_DAY3)
    print("\n=== RECONCILIATION ===")
    print(recon)

    _restore_victim_label(spark, cfg, victim, original)
    final = _snapshot_stats(spark, cfg, y, m, d)
    print("\n=== RESTORED STAGING SNAPSHOT ===")
    print(final)

    metrics = {
        "victim_transaction_id": txn_id,
        "event_date": event_date.isoformat(),
        "knowledge_date_day3": KNOWLEDGE_DAY3.isoformat(),
        "day1_fraud_txn": before["fraud_txn"],
        "day1_fraud_amount": round(before["fraud_amount"], 2),
        "day1_total_txn": before["total_txn"],
        "day3_fraud_txn": after["fraud_txn"],
        "day3_fraud_amount": round(after["fraud_amount"], 2),
        "fraud_txn_delta": after["fraud_txn"] - before["fraud_txn"],
        "fraud_amount_delta": round(after["fraud_amount"] - before["fraud_amount"], 2),
        "partitions_restated": restate_day3.get("partitions", 0),
        "changes_applied": restate_day3.get("applied", 0),
        "unexplained_difference": recon.get("unexplained_difference"),
        "invariant_ok": recon.get("invariant_ok"),
        "restored_fraud_txn": final["fraud_txn"],
    }

    print("\n========== DEMO_METRICS (copy into README) ==========")
    for k, v in metrics.items():
        print(f"{k}: {v}")
    print("====================================================\n")
    return metrics


if __name__ == "__main__":
    from ingestion.spark_session import get_spark_session, stop_spark_session

    spark = get_spark_session("demo_late_label_story")
    try:
        result = main(spark)
        print(f"\n=== DONE: {result} ===")
    finally:
        stop_spark_session(spark)
