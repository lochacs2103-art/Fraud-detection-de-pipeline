"""
demo_late_label_story.py — Day1 → Day3 late-arriving fraud label demo.

Story:
  Day 1 (event): transaction exists, label = No / not fraud in reports.
  Day 3 (knowledge): label flips to Yes → detect impact → restate only
  that event partition → before/after snapshots → restore source.

Prints a DEMO_METRICS block for README.

Usage:
  spark-submit ... scripts/demo_late_label_story.py
"""

from __future__ import annotations

import os
import sys
from datetime import date, datetime
from pathlib import Path

import yaml
from pyspark.sql import functions as F
import structlog

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parent.parent))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from transformation.audit.detect_fraud_label_changes import detect_changes
from transformation.audit.restate_affected_partitions import restate
from transformation.audit.reconcile_batch import reconcile

logger = structlog.get_logger(__name__)

# Knowledge dates for the story (after baseline 2019-12-31)
KNOWLEDGE_DAY3 = date(2020, 1, 3)
KNOWLEDGE_RESTORE = date(2020, 1, 4)


def _cfg():
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        return yaml.safe_load(f)


def _pick_victim(spark, cfg) -> dict:
    """Pick a labeled-No txn that exists in the transaction index."""
    fraud_path = cfg["lake"]["raw"] + "/fraud_labels"
    index_path = cfg["tables"]["transaction_index"]["warehouse"]

    labels = spark.read.parquet(fraud_path) \
        .filter(F.upper(F.col("is_fraud")) == "NO") \
        .select("transaction_id") \
        .dropDuplicates(["transaction_id"])

    idx = spark.read.parquet(index_path).select(
        "transaction_id", "event_date", "year", "month", "day", "amount", "is_fraud"
    )

    victim = labels.join(idx, on="transaction_id", how="inner").limit(1).collect()
    if not victim:
        raise RuntimeError("No victim transaction found (need No-label ∩ transaction_index)")
    r = victim[0]
    return {
        "transaction_id": r["transaction_id"],
        "event_date": r["event_date"].isoformat() if r["event_date"] else None,
        "year": int(r["year"]),
        "month": int(r["month"]),
        "day": int(r["day"]),
        "amount": float(r["amount"]) if r["amount"] is not None else None,
        "is_fraud_before": r["is_fraud"],
    }


def _backup_and_flip(spark, cfg, txn_id: str) -> str:
    fraud_path = cfg["lake"]["raw"] + "/fraud_labels"
    backup_path = cfg["lake"]["staging"] + "/.tmp/fraud_labels_demo_backup"

    df = spark.read.parquet(fraud_path)
    df.write.mode("overwrite").option("compression", "snappy").parquet(backup_path)

    flipped = df.withColumn(
        "is_fraud",
        F.when(F.col("transaction_id") == txn_id, F.lit("Yes")).otherwise(F.col("is_fraud")),
    )
    flipped.write.mode("overwrite").option("compression", "snappy").parquet(fraud_path)
    logger.info("demo.flipped_label", transaction_id=txn_id, to="Yes")
    return backup_path


def _restore_labels(spark, cfg, backup_path: str) -> None:
    fraud_path = cfg["lake"]["raw"] + "/fraud_labels"
    spark.read.parquet(backup_path) \
        .write.mode("overwrite").option("compression", "snappy").parquet(fraud_path)
    logger.info("demo.restored_fraud_labels")


def _snapshot_stats(spark, cfg, y: int, m: int, d: int) -> dict:
    path = f"{cfg['tables']['transactions']['staging']}/year={y}/month={m}/day={d}"
    df = spark.read.parquet(path)
    row = df.agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("is_fraud") == True, 1).otherwise(0)).alias("fraud_n"),
        F.sum(F.when(F.col("is_fraud") == True, F.col("amount").cast("double")).otherwise(0.0))
         .alias("fraud_amt"),
    ).collect()[0]
    return {
        "total_txn": int(row["total"]),
        "fraud_txn": int(row["fraud_n"] or 0),
        "fraud_amount": float(row["fraud_amt"] or 0.0),
    }


def main(spark) -> dict:
    cfg = _cfg()
    victim = _pick_victim(spark, cfg)
    txn_id = victim["transaction_id"]
    y, m, d = victim["year"], victim["month"], victim["day"]
    event_date = date(y, m, d)

    print("\n=== DEMO VICTIM ===")
    print(victim)

    before = _snapshot_stats(spark, cfg, y, m, d)
    print("\n=== DAY1 REPORT (before late label) ===")
    print(before)

    backup = _backup_and_flip(spark, cfg, txn_id)

    detect_day3 = detect_changes(spark, KNOWLEDGE_DAY3)
    print("\n=== DAY3 DETECT ===")
    print(detect_day3)

    restate_day3 = restate(spark, KNOWLEDGE_DAY3)
    print("\n=== DAY3 RESTATE ===")
    print(restate_day3)

    after = _snapshot_stats(spark, cfg, y, m, d)
    print("\n=== DAY3 REPORT (after restatement) ===")
    print(after)

    # Impact row for victim
    manifest_path = cfg["tables"]["fraud_label_impact_manifest"]["warehouse"]
    impact = spark.read.parquet(manifest_path) \
        .filter(F.col("transaction_id") == txn_id) \
        .orderBy(F.col("knowledge_date").desc()) \
        .limit(1)
    impact_rows = [r.asDict() for r in impact.collect()]
    print("\n=== IMPACT MANIFEST (victim) ===")
    print(impact_rows)

    recon = reconcile(spark, event_date, KNOWLEDGE_DAY3)
    print("\n=== RECONCILIATION ===")
    print(recon)

    # Restore source labels + restate reverse so lake returns to baseline story state
    _restore_labels(spark, cfg, backup)
    detect_restore = detect_changes(spark, KNOWLEDGE_RESTORE)
    restate_restore = restate(spark, KNOWLEDGE_RESTORE)
    print("\n=== RESTORE DETECT/RESTATE ===")
    print(detect_restore)
    print(restate_restore)

    final = _snapshot_stats(spark, cfg, y, m, d)

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
