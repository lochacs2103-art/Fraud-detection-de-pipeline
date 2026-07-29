"""
detect_fraud_label_changes.py — Late-arriving / changed fraud labels.

Modes:
  --baseline YYYY-MM-DD
      First run: ghi toàn bộ fraud_labels vào fraud_label_history.
      KHÔNG tạo impact_manifest (baseline ≠ late change).

  YYYY-MM-DD
      So sánh raw fraud_labels với latest history:
        - NEW_LABEL / LABEL_CHANGED / LABEL_CLEARED → impact_manifest
        - Append trạng thái mới vào fraud_label_history
      Join transaction_index để lấy event_date + partition keys.

Output:
  warehouse.fraud_label_history
  warehouse.fraud_label_impact_manifest
"""

from __future__ import annotations

import os
import sys
from datetime import date, datetime
from pathlib import Path

import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import structlog

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parent.parent.parent))
logger = structlog.get_logger(__name__)


def _load_cfg() -> dict:
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        return yaml.safe_load(f)


def _cast_label(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("transaction_id"),
        F.col("is_fraud").alias("is_fraud_raw"),
        F.when(F.upper(F.col("is_fraud")) == "YES", F.lit(True))
         .when(F.upper(F.col("is_fraud")) == "NO", F.lit(False))
         .otherwise(F.lit(None).cast("boolean"))
         .alias("is_fraud"),
    ).dropDuplicates(["transaction_id"])


def _read_latest_history(spark: SparkSession, history_path: str) -> DataFrame | None:
    try:
        hist = spark.read.parquet(history_path)
    except Exception:
        return None
    if not hist.head(1):
        return None

    w = Window.partitionBy("transaction_id").orderBy(
        F.col("knowledge_date").desc(),
        F.col("_recorded_at").desc(),
    )
    return (
        hist.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            F.col("transaction_id"),
            F.col("is_fraud").alias("old_is_fraud"),
            F.col("is_fraud_raw").alias("old_is_fraud_raw"),
        )
    )


def write_baseline(spark: SparkSession, knowledge_date: date) -> dict:
    cfg = _load_cfg()
    fraud_path = cfg["lake"]["raw"] + "/fraud_labels"
    history_path = cfg["tables"]["fraud_label_history"]["warehouse"]
    batch_id = f"fraud_baseline_{knowledge_date.isoformat()}"

    logger.info("fraud_label_changes.baseline_start", knowledge_date=knowledge_date.isoformat())

    labels = _cast_label(spark.read.parquet(fraud_path))
    history = labels.select(
        F.col("transaction_id"),
        F.col("is_fraud"),
        F.col("is_fraud_raw"),
        F.lit(knowledge_date).alias("knowledge_date"),
        F.lit(batch_id).alias("source_batch_id"),
        F.current_timestamp().alias("_recorded_at"),
        F.lit(knowledge_date.year).alias("knowledge_year"),
        F.lit(knowledge_date.month).alias("knowledge_month"),
        F.lit(knowledge_date.day).alias("knowledge_day"),
    )

    count = history.count()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    history.repartition(10) \
        .write.mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("knowledge_year", "knowledge_month", "knowledge_day") \
        .parquet(history_path)

    logger.info("fraud_label_changes.baseline_done", rows=count)
    return {
        "mode": "baseline",
        "knowledge_date": knowledge_date.isoformat(),
        "history_rows": count,
        "impact_rows": 0,
        "batch_id": batch_id,
    }


def detect_changes(spark: SparkSession, knowledge_date: date) -> dict:
    cfg = _load_cfg()
    fraud_path = cfg["lake"]["raw"] + "/fraud_labels"
    history_path = cfg["tables"]["fraud_label_history"]["warehouse"]
    manifest_path = cfg["tables"]["fraud_label_impact_manifest"]["warehouse"]
    index_path = cfg["tables"]["transaction_index"]["warehouse"]
    batch_id = f"fraud_detect_{knowledge_date.isoformat()}_{datetime.utcnow().strftime('%H%M%S')}"

    logger.info("fraud_label_changes.detect_start", knowledge_date=knowledge_date.isoformat())

    current = _cast_label(spark.read.parquet(fraud_path)) \
        .withColumnRenamed("is_fraud", "new_is_fraud") \
        .withColumnRenamed("is_fraud_raw", "new_is_fraud_raw")

    latest = _read_latest_history(spark, history_path)
    if latest is None:
        logger.warning("fraud_label_changes.no_history_run_baseline")
        return write_baseline(spark, knowledge_date)

    # Changes: new label or value differs (null-safe)
    joined = current.join(latest, on="transaction_id", how="left")
    changed = joined.filter(
        F.col("old_is_fraud").isNull()
        | (F.col("old_is_fraud") != F.col("new_is_fraud"))
        | (F.col("old_is_fraud").isNotNull() & F.col("new_is_fraud").isNull())
    ).withColumn(
        "change_type",
        F.when(F.col("old_is_fraud").isNull() & F.col("new_is_fraud").isNotNull(), F.lit("NEW_LABEL"))
         .when(F.col("old_is_fraud").isNotNull() & F.col("new_is_fraud").isNull(), F.lit("LABEL_CLEARED"))
         .otherwise(F.lit("LABEL_CHANGED"))
    )

    # Attach event partition from transaction_index
    try:
        idx = spark.read.parquet(index_path).select(
            "transaction_id", "event_date", "year", "month", "day"
        )
    except Exception as e:
        logger.error("fraud_label_changes.index_missing", error=str(e))
        raise

    impact = changed.join(idx, on="transaction_id", how="inner") \
        .withColumn("change_id", F.concat(
            F.lit(knowledge_date.isoformat()), F.lit("_"),
            F.col("transaction_id"), F.lit("_"),
            F.coalesce(F.col("change_type"), F.lit("UNK")),
        )) \
        .withColumn("knowledge_date", F.lit(knowledge_date)) \
        .withColumn("_detected_at", F.current_timestamp()) \
        .withColumn("_batch_id", F.lit(batch_id)) \
        .withColumn("knowledge_year", F.lit(knowledge_date.year)) \
        .withColumn("knowledge_month", F.lit(knowledge_date.month)) \
        .withColumn("knowledge_day", F.lit(knowledge_date.day)) \
        .select(
            "change_id", "transaction_id", "event_date", "knowledge_date",
            F.col("old_is_fraud"),
            F.col("new_is_fraud"),
            "year", "month", "day",
            "change_type", "_detected_at", "_batch_id",
            "knowledge_year", "knowledge_month", "knowledge_day",
        )

    impact_count = impact.count()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    impact.repartition(4) \
        .write.mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("knowledge_year", "knowledge_month", "knowledge_day") \
        .parquet(manifest_path)

    # Append current snapshot rows that changed (or all current as new knowledge?)
    # Spec: history records every known state at knowledge_date for changed + new labels only
    hist_append = changed.select(
        F.col("transaction_id"),
        F.col("new_is_fraud").alias("is_fraud"),
        F.col("new_is_fraud_raw").alias("is_fraud_raw"),
        F.lit(knowledge_date).alias("knowledge_date"),
        F.lit(batch_id).alias("source_batch_id"),
        F.current_timestamp().alias("_recorded_at"),
        F.lit(knowledge_date.year).alias("knowledge_year"),
        F.lit(knowledge_date.month).alias("knowledge_month"),
        F.lit(knowledge_date.day).alias("knowledge_day"),
    )

    if hist_append.head(1):
        hist_append.repartition(4) \
            .write.mode("append") \
            .option("compression", "snappy") \
            .partitionBy("knowledge_year", "knowledge_month", "knowledge_day") \
            .parquet(history_path)

    logger.info(
        "fraud_label_changes.detect_done",
        impact_rows=impact_count,
        knowledge_date=knowledge_date.isoformat(),
    )
    return {
        "mode": "detect",
        "knowledge_date": knowledge_date.isoformat(),
        "impact_rows": impact_count,
        "batch_id": batch_id,
    }


if __name__ == "__main__":
    from ingestion.spark_session import get_spark_session, stop_spark_session

    if len(sys.argv) < 2:
        print("Usage: detect_fraud_label_changes.py --baseline YYYY-MM-DD | YYYY-MM-DD")
        sys.exit(1)

    spark = get_spark_session("detect_fraud_label_changes")
    try:
        if sys.argv[1] == "--baseline":
            if len(sys.argv) < 3:
                print("Usage: detect_fraud_label_changes.py --baseline YYYY-MM-DD")
                sys.exit(1)
            kd = date.fromisoformat(sys.argv[2])
            result = write_baseline(spark, kd)
        else:
            kd = date.fromisoformat(sys.argv[1])
            result = detect_changes(spark, kd)
        print(f"\n=== DONE: {result} ===")
    finally:
        stop_spark_session(spark)
