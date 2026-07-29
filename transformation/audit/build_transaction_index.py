"""
build_transaction_index.py — Narrow transaction_id → event partition lookup.

Modes:
  --full              Bootstrap toàn bộ staging.transactions (theo năm)
  YYYY-MM-DD          Chỉ index 1 ngày (daily incremental)

Output: warehouse.transaction_index (Parquet, partitioned year/month/day)
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

ALL_YEARS = list(range(2010, 2020))


def _load_cfg() -> dict:
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        return yaml.safe_load(f)


def _to_index(df: DataFrame, batch_id: str) -> DataFrame:
    return df.select(
        F.col("transaction_id"),
        F.col("user_id"),
        F.to_date("transaction_date").alias("event_date"),
        F.col("transaction_date"),
        F.col("amount").cast("double").alias("amount"),
        F.col("is_fraud"),
        F.current_timestamp().alias("_indexed_at"),
        F.lit(batch_id).alias("_batch_id"),
        F.col("year"),
        F.col("month"),
        F.col("day"),
    )


def build_index_year(spark: SparkSession, year: int, staging_path: str, index_path: str, batch_id: str) -> int:
    year_path = f"{staging_path}/year={year}"
    try:
        df = spark.read.option("basePath", staging_path).parquet(year_path)
    except Exception as e:
        logger.warning("transaction_index.year_missing", year=year, error=str(e))
        return 0

    if not df.head(1):
        return 0

    indexed = _to_index(df.filter(F.col("is_valid") == True), batch_id)
    count = indexed.count()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    indexed.repartition("year", "month", "day") \
        .write.mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("year", "month", "day") \
        .parquet(index_path)

    logger.info("transaction_index.year_done", year=year, rows=count)
    return count


def build_index_day(spark: SparkSession, exec_date: date, staging_path: str, index_path: str, batch_id: str) -> int:
    part = (
        f"{staging_path}/year={exec_date.year}"
        f"/month={exec_date.month}/day={exec_date.day}"
    )
    df = spark.read.parquet(part)
    indexed = _to_index(df.filter(F.col("is_valid") == True), batch_id)
    count = indexed.count()

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    indexed.repartition("year", "month", "day") \
        .write.mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("year", "month", "day") \
        .parquet(index_path)

    logger.info("transaction_index.day_done", date=exec_date.isoformat(), rows=count)
    return count


def build_transaction_index(spark: SparkSession, mode: str) -> dict:
    cfg = _load_cfg()
    staging_path = cfg["tables"]["transactions"]["staging"]
    index_path = cfg["tables"]["transaction_index"]["warehouse"]
    batch_id = f"txn_index_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

    spark.conf.set("spark.sql.shuffle.partitions", "20")
    logger.info("transaction_index.start", mode=mode, batch_id=batch_id)

    if mode == "--full":
        total = 0
        for year in ALL_YEARS:
            total += build_index_year(spark, year, staging_path, index_path, batch_id)
        return {"mode": "full", "rows": total, "batch_id": batch_id}

    exec_date = date.fromisoformat(mode)
    rows = build_index_day(spark, exec_date, staging_path, index_path, batch_id)
    return {"mode": "daily", "date": mode, "rows": rows, "batch_id": batch_id}


if __name__ == "__main__":
    from ingestion.spark_session import get_spark_session, stop_spark_session

    if len(sys.argv) < 2:
        print("Usage: build_transaction_index.py --full | YYYY-MM-DD")
        sys.exit(1)

    spark = get_spark_session("build_transaction_index")
    try:
        result = build_transaction_index(spark, sys.argv[1])
        print(f"\n=== DONE: {result} ===")
    finally:
        stop_spark_session(spark)
