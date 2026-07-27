"""
enrich_fraud_labels.py — Join fraud_labels vào staging transactions theo từng năm.

Strategy: hash-mod batch + materialized fraud partitions.
- Ghi fraud_labels ra HDFS partitioned by _batch (1 lần duy nhất)
- Mỗi batch: đọc partition nhỏ (~890K) + repartition join (không broadcast)
- Tránh: broadcast 8.9M, re-scan fraud 100 lần/năm, driver/executor OOM

Debug nhanh (không cần chạy full 10 năm):
  ENRICH_FRAUD_DEBUG_YEAR=2010 ENRICH_FRAUD_DEBUG_BATCH=0
  ENRICH_FRAUD_SKIP_WRITE=1   # optional — join xong không ghi HDFS
"""

import os
from functools import reduce
from pathlib import Path

import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import structlog

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).parent.parent.parent))
logger = structlog.get_logger(__name__)

ALL_YEARS = [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019]
NUM_BATCHES = 10
JOIN_PARTITIONS = 8


def _debug_years() -> list[int]:
    raw = os.environ.get("ENRICH_FRAUD_DEBUG_YEAR")
    if not raw:
        return ALL_YEARS
    return [int(raw)]


def _debug_batches(num_batches: int) -> range:
    raw = os.environ.get("ENRICH_FRAUD_DEBUG_BATCH")
    if raw is None:
        return range(num_batches)
    return range(int(raw), int(raw) + 1)


def _skip_write() -> bool:
    return os.environ.get("ENRICH_FRAUD_SKIP_WRITE", "").lower() in ("1", "true", "yes")


def _cast_fraud_label(df: DataFrame) -> DataFrame:
    """Cast is_fraud_raw (Yes/No) → BOOLEAN."""
    return df.withColumn(
        "is_fraud",
        F.when(F.upper(F.col("is_fraud_raw")) == "YES", F.lit(True))
         .when(F.upper(F.col("is_fraud_raw")) == "NO",  F.lit(False))
         .otherwise(F.lit(None).cast("boolean"))
    ).drop("is_fraud_raw")


def _join_batch(txn_batch: DataFrame, fraud_batch: DataFrame) -> DataFrame:
    """Co-partitioned join — không broadcast, mỗi partition ~100K rows."""
    txn_part = txn_batch.repartition(JOIN_PARTITIONS, "transaction_id")
    fraud_part = fraud_batch.repartition(JOIN_PARTITIONS, "transaction_id")
    return txn_part.join(fraud_part, on="transaction_id", how="left")


def _materialize_fraud_batches(
    spark: SparkSession,
    fraud_labels_path: str,
    batched_path: str,
    num_batches: int,
) -> int:
    """Scan fraud_labels 1 lần, ghi partitioned by _batch để batch read nhanh."""
    logger.info("enrich_fraud_labels.materialize_batches.start", path=batched_path)

    fraud_df = spark.read.parquet(fraud_labels_path) \
        .select(
            F.col("transaction_id"),
            F.col("is_fraud").alias("is_fraud_raw"),
        ) \
        .dropDuplicates(["transaction_id"]) \
        .withColumn(
            "_batch",
            F.pmod(F.hash("transaction_id"), F.lit(num_batches)),
        )

    fraud_count = fraud_df.count()
    logger.info("enrich_fraud_labels.fraud_loaded", count=fraud_count, num_batches=num_batches)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    fraud_df.repartition(num_batches, "_batch") \
        .write.mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("_batch") \
        .parquet(batched_path)

    logger.info("enrich_fraud_labels.materialize_batches.done", count=fraud_count)
    return fraud_count


def _read_fraud_batch(spark: SparkSession, batched_path: str, batch_id: int) -> DataFrame:
    return spark.read.parquet(f"{batched_path}/_batch={batch_id}") \
        .drop("_batch")


def enrich_year_fraud(
    spark,
    year: int,
    staging_path: str,
    batched_path: str,
    num_batches: int,
    batch_range: range,
) -> int:
    """Join fraud labels vào 1 năm staging transactions theo hash batches."""

    year_path = f"{staging_path}/year={year}"

    try:
        df = spark.read \
            .option("basePath", staging_path) \
            .parquet(year_path)
    except Exception as e:
        logger.warning("enrich_fraud_labels.year_not_found", year=year, error=str(e))
        return 0

    if not df.head(1):
        logger.info("enrich_fraud_labels.year_empty", year=year)
        return 0

    if "is_fraud" in df.columns:
        df = df.drop("is_fraud")

    df = df.withColumn(
        "_batch",
        F.pmod(F.hash("transaction_id"), F.lit(num_batches)),
    )

    enriched_parts = []
    for batch_id in batch_range:
        txn_batch = df.filter(F.col("_batch") == batch_id).drop("_batch")
        fraud_batch = _read_fraud_batch(spark, batched_path, batch_id)
        joined = _join_batch(txn_batch, fraud_batch)
        enriched_parts.append(_cast_fraud_label(joined))

        matched = joined.filter(F.col("is_fraud_raw").isNotNull()).count()
        logger.info(
            "enrich_fraud_labels.batch_done",
            year=year,
            batch=batch_id,
            matched=matched,
        )

    if not enriched_parts:
        logger.info("enrich_fraud_labels.year_empty_after_batching", year=year)
        return 0

    result = reduce(DataFrame.unionByName, enriched_parts)

    if _skip_write():
        total = result.count()
        fraud_labeled = result.filter(F.col("is_fraud").isNotNull()).count()
        logger.info(
            "enrich_fraud_labels.skip_write",
            year=year,
            rows=total,
            labeled=fraud_labeled,
        )
        return 1

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    result.repartition(F.col("year"), F.col("month"), F.col("day")) \
        .write.mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("year", "month", "day") \
        .parquet(staging_path)

    logger.info("enrich_fraud_labels.year_done", year=year, batches=len(enriched_parts))
    return 1


def enrich_fraud_labels(spark: SparkSession) -> dict:
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        cfg = yaml.safe_load(f)

    staging_path = cfg["tables"]["transactions"]["staging"]
    fraud_labels_path = cfg["lake"]["raw"] + "/fraud_labels"
    batched_path = cfg["lake"]["staging"] + "/.tmp/fraud_labels_batched"

    years = _debug_years()
    batch_range = _debug_batches(NUM_BATCHES)

    spark.conf.set("spark.sql.shuffle.partitions", "20")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    logger.info(
        "enrich_fraud_labels.start",
        years=years,
        batches=list(batch_range),
        skip_write=_skip_write(),
    )

    fraud_count = _materialize_fraud_batches(
        spark, fraud_labels_path, batched_path, NUM_BATCHES
    )

    total = 0
    for year in years:
        logger.info("enrich_fraud_labels.processing_year", year=year)
        count = enrich_year_fraud(
            spark, year, staging_path, batched_path, NUM_BATCHES, batch_range
        )
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
