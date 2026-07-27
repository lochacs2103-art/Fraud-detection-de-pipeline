"""
enrich_fraud_labels.py — Join fraud_labels vào staging transactions theo từng năm.

Strategy: Spark broadcast join với fraud_labels.
- Select chỉ 2 columns + dropDuplicates → giảm size đáng kể
- Force broadcast qua F.broadcast() — Spark copy fraud_df lên mỗi executor
- Không collect về driver, không sort-merge shuffle
- Mỗi năm: đọc ~1.3M rows, broadcast join → write parquet
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

    try:
        df = spark.read \
            .option("basePath", staging_path) \
            .parquet(year_path)
    except Exception as e:
        logger.warning("enrich_fraud_labels.year_not_found", year=year, error=str(e))
        return 0

    if not df.take(1):
        logger.info("enrich_fraud_labels.year_empty", year=year)
        return 0

    # Drop is_fraud nếu đã có (idempotent)
    if "is_fraud" in df.columns:
        df = df.drop("is_fraud")

    # Force broadcast join — fraud_df nhỏ hơn transactions
    df = df.join(F.broadcast(fraud_df), on="transaction_id", how="left")

    # Cast "Yes"/"No" → BOOLEAN
    df = df.withColumn(
        "is_fraud",
        F.when(F.upper(F.col("is_fraud_raw")) == "YES", F.lit(True))
         .when(F.upper(F.col("is_fraud_raw")) == "NO",  F.lit(False))
         .otherwise(F.lit(None).cast("boolean"))
    ).drop("is_fraud_raw")

    # Write lại partition của năm này
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df.repartition(F.col("year"), F.col("month"), F.col("day")) \
      .write.mode("overwrite") \
      .option("compression", "snappy") \
      .partitionBy("year", "month", "day") \
      .parquet(staging_path)

    logger.info("enrich_fraud_labels.year_done", year=year)
    return 1


def enrich_fraud_labels(spark: SparkSession) -> dict:
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        cfg = yaml.safe_load(f)

    staging_path      = cfg["tables"]["transactions"]["staging"]
    fraud_labels_path = cfg["lake"]["raw"] + "/fraud_labels"

    spark.conf.set("spark.sql.shuffle.partitions", "20")

    logger.info("enrich_fraud_labels.start")

    # Load fraud labels — chỉ 2 columns sau dropDuplicates
    # Spark broadcast sẽ serialize và copy lên executors
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
        logger.info("enrich_fraud_labels.progress", year=year, total_so_far=total)

    logger.info("enrich_fraud_labels.done", years_processed=total)
    return {"years_processed": total, "fraud_labels_loaded": fraud_count}


if __name__ == "__main__":
    from ingestion.spark_session import get_spark_session, stop_spark_session

    spark = get_spark_session("enrich_fraud_labels")
    try:
        result = enrich_fraud_labels(spark)
        print(f"\n=== DONE: {result} ===")
    finally:
        stop_spark_session(spark)
