"""
enrich_fraud_labels.py — Join fraud_labels vào staging transactions.

Tại sao không làm ở bước enrich_transactions_full.py?
- fraud_labels có 8.9M rows → quá lớn để broadcast (threshold 50MB)
- Sort-merge join 13.3M × 8.9M trong 1 job → OOM với 2GB executor

Strategy: loop theo năm.
- Mỗi năm: load ~1.3M transactions + 8.9M fraud_labels → sort-merge join
- AQE tự optimize shuffle partitions
- Overwrite đúng partition (dynamic partition overwrite)

is_fraud: "Yes" → True, "No" → False, NULL → NULL (label chưa có)
"""

import os
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import structlog

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).parent.parent.parent))
logger = structlog.get_logger(__name__)

YEARS = [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019]


def enrich_year_fraud(spark, year, staging_path, fraud_df):
    """Join fraud labels vào 1 năm staging transactions."""

    year_path = f"{staging_path}/year={year}"

    # Đọc staging của năm này
    try:
        df = spark.read.parquet(year_path)
    except Exception:
        logger.warning("enrich_fraud_labels.year_not_found", year=year)
        return 0

    # Drop is_fraud nếu đã có (idempotent)
    if "is_fraud" in df.columns:
        df = df.drop("is_fraud")

    # Sort-merge join — AQE tự handle skew và coalesce shuffle partitions
    df = df.join(fraud_df, on="transaction_id", how="left")

    # Cast "Yes"/"No" → BOOLEAN
    df = df.withColumn(
        "is_fraud",
        F.when(F.upper(F.col("is_fraud_raw")) == "YES", F.lit(True))
         .when(F.upper(F.col("is_fraud_raw")) == "NO",  F.lit(False))
         .otherwise(F.lit(None).cast("boolean"))
    ).drop("is_fraud_raw")

    # Write lại đúng partition — dynamic overwrite chỉ đụng partition của năm này
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df.repartition(F.col("year"), F.col("month"), F.col("day")) \
      .write.mode("overwrite") \
      .option("compression", "snappy") \
      .partitionBy("year", "month", "day") \
      .parquet(staging_path)

    count = df.count()
    logger.info("enrich_fraud_labels.year_done", year=year, count=count)
    return count


def enrich_fraud_labels(spark: SparkSession) -> dict:
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        cfg = yaml.safe_load(f)

    staging_path   = cfg["tables"]["transactions"]["staging"]
    fraud_labels_path = cfg["lake"]["raw"] + "/fraud_labels"

    spark.conf.set("spark.sql.shuffle.partitions", "50")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

    logger.info("enrich_fraud_labels.start")

    # Load fraud labels 1 lần — dùng lại cho tất cả các năm
    fraud_df = spark.read.parquet(fraud_labels_path) \
        .select(
            F.col("transaction_id"),
            F.col("is_fraud").alias("is_fraud_raw")
        ) \
        .dropDuplicates(["transaction_id"])

    fraud_count = fraud_df.count()
    logger.info("enrich_fraud_labels.fraud_loaded", count=fraud_count)

    # Cache fraud_labels — dùng lại nhiều lần, nhưng spill to disk nếu cần
    from pyspark import StorageLevel
    fraud_df.persist(StorageLevel.MEMORY_AND_DISK)

    total = 0
    for year in YEARS:
        logger.info("enrich_fraud_labels.processing_year", year=year)
        count = enrich_year_fraud(spark, year, staging_path, fraud_df)
        total += count
        logger.info("enrich_fraud_labels.progress", year=year, total_so_far=total)

    fraud_df.unpersist()

    logger.info("enrich_fraud_labels.done", total=total)
    return {"total": total, "fraud_labels_loaded": fraud_count}


if __name__ == "__main__":
    from ingestion.spark_session import get_spark_session, stop_spark_session

    spark = get_spark_session("enrich_fraud_labels")
    try:
        result = enrich_fraud_labels(spark)
        print(f"\n=== DONE: {result} ===")
    finally:
        stop_spark_session(spark)
