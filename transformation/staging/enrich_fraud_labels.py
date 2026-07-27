"""
enrich_fraud_labels.py — Join fraud_labels vào staging transactions theo từng năm.

Strategy:
- Đọc từng year partition riêng lẻ (không load 13.3M rows cùng lúc)
- Dùng basePath để Spark tự thêm partition columns (year/month/day)
- Sort-merge join với fraud_labels (8.9M rows, cached MEMORY_AND_DISK)
- Overwrite đúng partition sau khi join
"""

import os
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import StorageLevel
import structlog

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).parent.parent.parent))
logger = structlog.get_logger(__name__)

YEARS = [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019]


def enrich_year_fraud(spark, year, staging_path, fraud_df):
    """Join fraud labels vào 1 năm staging transactions."""

    year_path = f"{staging_path}/year={year}"

    # Dùng basePath để Spark tự include partition columns (year/month/day)
    try:
        df = spark.read \
            .option("basePath", staging_path) \
            .parquet(year_path)
    except Exception as e:
        logger.warning("enrich_fraud_labels.year_not_found", year=year, error=str(e))
        return 0

    row_count = 0  # không count() để tránh re-execute lineage
    if not df.take(1):  # fast check — chỉ lấy 1 row
        logger.info("enrich_fraud_labels.year_empty", year=year)
        return 0

    # Drop is_fraud nếu đã có (idempotent)
    if "is_fraud" in df.columns:
        df = df.drop("is_fraud")

    # Join fraud labels — AQE + broadcast threshold 300MB sẽ tự chọn broadcast join
    df = df.join(fraud_df, on="transaction_id", how="left")

    # Cast "Yes"/"No" → BOOLEAN
    df = df.withColumn(
        "is_fraud",
        F.when(F.upper(F.col("is_fraud_raw")) == "YES", F.lit(True))
         .when(F.upper(F.col("is_fraud_raw")) == "NO",  F.lit(False))
         .otherwise(F.lit(None).cast("boolean"))
    ).drop("is_fraud_raw")

    # Write lại — dynamic overwrite chỉ đụng partitions của năm này
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df.repartition(F.col("year"), F.col("month"), F.col("day")) \
      .write.mode("overwrite") \
      .option("compression", "snappy") \
      .partitionBy("year", "month", "day") \
      .parquet(staging_path)

    logger.info("enrich_fraud_labels.year_done", year=year, count=row_count)
    return row_count


def enrich_fraud_labels(spark: SparkSession) -> dict:
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        cfg = yaml.safe_load(f)

    staging_path      = cfg["tables"]["transactions"]["staging"]
    fraud_labels_path = cfg["lake"]["raw"] + "/fraud_labels"

    # Giảm shuffle partitions — mỗi year ~1.3M rows không cần nhiều partitions
    spark.conf.set("spark.sql.shuffle.partitions", "20")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    # Tăng broadcast threshold — fraud_labels 2 columns ~200MB uncompressed
    # Broadcast tốt hơn sort-merge join với 2GB executor
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(300 * 1024 * 1024))

    logger.info("enrich_fraud_labels.start")

    # Load fraud labels 1 lần, cache để dùng lại cho tất cả 10 năm
    # Không persist vào executor memory — dùng broadcast join thay thế
    # autoBroadcastJoinThreshold=300MB được set qua spark-submit --conf
    fraud_df = spark.read.parquet(fraud_labels_path) \
        .select(
            F.col("transaction_id"),
            F.col("is_fraud").alias("is_fraud_raw")
        ) \
        .dropDuplicates(["transaction_id"])

    fraud_count = fraud_df.count()
    logger.info("enrich_fraud_labels.fraud_loaded", count=fraud_count)

    total = 0
    for year in YEARS:
        logger.info("enrich_fraud_labels.processing_year", year=year)
        count = enrich_year_fraud(spark, year, staging_path, fraud_df)
        total += count
        logger.info("enrich_fraud_labels.progress",
                    year=year, year_count=count, total_so_far=total)

    fraud_df.unpersist() if fraud_df.is_cached else None

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
