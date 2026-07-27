"""
enrich_fraud_labels.py — Join fraud_labels vào staging transactions theo từng năm.

Strategy: collect fraud_labels về driver dict, dùng Pandas UDF để lookup.
- Tránh sort-merge join 8.9M × 1.3M → không OOM, không heartbeat timeout
- Pandas UDF vectorized: nhanh hơn Python UDF row-by-row
- Fraud_labels dict fit vào driver memory (2 columns × 8.9M rows ≈ 300MB)
"""

import os
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
import structlog

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).parent.parent.parent))
logger = structlog.get_logger(__name__)

YEARS = [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019]


def enrich_year_fraud(spark, year, staging_path, fraud_broadcast):
    """Join fraud labels vào 1 năm staging transactions dùng broadcast lookup."""

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

    # Pandas UDF dùng broadcast variable — không shuffle, không sort-merge join
    import pandas as pd
    from pyspark.sql.functions import pandas_udf

    @pandas_udf(BooleanType())
    def lookup_fraud(txn_ids: pd.Series) -> pd.Series:
        fraud_dict = fraud_broadcast.value
        return txn_ids.map(lambda tid: fraud_dict.get(str(tid)))

    df = df.withColumn("is_fraud", lookup_fraud(F.col("transaction_id")))

    # Write lại — dynamic overwrite chỉ đụng partitions của năm này
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

    # Collect fraud_labels về driver → Python dict
    # 8.9M rows × (transaction_id + is_fraud) ≈ 300MB — fit driver 1GB
    logger.info("enrich_fraud_labels.collecting_fraud_labels")
    fraud_rows = spark.read.parquet(fraud_labels_path) \
        .select("transaction_id", "is_fraud") \
        .dropDuplicates(["transaction_id"]) \
        .collect()

    # Build dict: transaction_id → True/False/None
    fraud_dict = {}
    for row in fraud_rows:
        val = row["is_fraud"]
        if val is not None:
            fraud_dict[str(row["transaction_id"])] = (val.upper() == "YES") if isinstance(val, str) else bool(val)

    fraud_count = len(fraud_dict)
    logger.info("enrich_fraud_labels.fraud_dict_built", count=fraud_count)

    # Broadcast dict lên tất cả executors — 1 lần duy nhất
    fraud_broadcast = spark.sparkContext.broadcast(fraud_dict)

    total = 0
    for year in YEARS:
        logger.info("enrich_fraud_labels.processing_year", year=year)
        count = enrich_year_fraud(spark, year, staging_path, fraud_broadcast)
        total += count
        logger.info("enrich_fraud_labels.progress", year=year, total_so_far=total)

    fraud_broadcast.unpersist()

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
