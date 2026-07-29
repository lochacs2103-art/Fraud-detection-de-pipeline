"""
build_fraud_features.py — Velocity / anomaly features (point-in-time safe).

Invariant: a transaction must NOT contribute to its own historical features.
Windows include the current row then subtract self (count-1, sum-amount),
and user avg uses rowsBefore current only.
"""

from pathlib import Path
from datetime import date, timedelta

import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import structlog

logger = structlog.get_logger(__name__)
PROJECT_ROOT = Path(__file__).parent.parent.parent


def _apply_pit_features(df: DataFrame) -> DataFrame:
    """Add PIT velocity / anomaly columns. Current txn excluded from history."""
    df = df.withColumn("ts", F.unix_timestamp("transaction_date"))

    w_user = Window.partitionBy("user_id").orderBy("ts", "transaction_id")
    # Include current row in range, then subtract self → exclude leakage
    w_1h = w_user.rangeBetween(-3600, Window.currentRow)
    w_24h = w_user.rangeBetween(-86400, Window.currentRow)
    w_7d = w_user.rangeBetween(-604800, Window.currentRow)
    # Prior rows only (never includes current)
    w_prior = w_user.rowsBetween(Window.unboundedPreceding, -1)

    return (
        df
        .withColumn("_cnt_1h", F.count("transaction_id").over(w_1h))
        .withColumn("_cnt_24h", F.count("transaction_id").over(w_24h))
        .withColumn("_cnt_7d", F.count("transaction_id").over(w_7d))
        .withColumn("_sum_1h", F.sum("amount").over(w_1h))
        .withColumn("_sum_24h", F.sum("amount").over(w_24h))
        .withColumn("txn_count_last_1h", F.col("_cnt_1h") - 1)
        .withColumn("txn_count_last_24h", F.col("_cnt_24h") - 1)
        .withColumn("txn_count_last_7d", F.col("_cnt_7d") - 1)
        .withColumn("amount_sum_last_1h", F.col("_sum_1h") - F.col("amount"))
        .withColumn("amount_sum_last_24h", F.col("_sum_24h") - F.col("amount"))
        .withColumn("user_avg_amount", F.avg("amount").over(w_prior))
        .withColumn(
            "amount_vs_user_avg_ratio",
            F.when(
                F.col("user_avg_amount").isNotNull() & (F.col("user_avg_amount") > 0),
                F.col("amount") / F.col("user_avg_amount"),
            ).otherwise(F.lit(None)),
        )
        .withColumn("is_night_txn", F.hour("transaction_date").between(0, 5))
        .withColumn("is_weekend", F.dayofweek("transaction_date").isin([1, 7]))
        .withColumn(
            "is_foreign_merchant",
            ~F.col("merchant_state").isin([
                "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
                "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
                "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
                "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
                "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
            ]),
        )
        .drop("_cnt_1h", "_cnt_24h", "_cnt_7d", "_sum_1h", "_sum_24h")
    )


def _select_feature_cols(df: DataFrame) -> DataFrame:
    card_col = (
        F.col("card_on_dark_web")
        if "card_on_dark_web" in df.columns
        else F.lit(None).cast("boolean")
    )
    fraud_col = (
        F.col("is_fraud")
        if "is_fraud" in df.columns
        else F.lit(None).cast("boolean")
    )
    return df.select(
        "transaction_id", "user_id",
        "txn_count_last_1h", "txn_count_last_24h", "txn_count_last_7d",
        F.col("amount_sum_last_1h").cast("double").alias("amount_sum_last_1h"),
        F.col("amount_sum_last_24h").cast("double").alias("amount_sum_last_24h"),
        F.col("amount_vs_user_avg_ratio").cast("double").alias("amount_vs_user_avg_ratio"),
        "is_night_txn", "is_weekend", "is_foreign_merchant",
        card_col.alias("card_on_dark_web"),
        fraud_col.alias("is_fraud"),
        F.lit(None).cast("double").alias("risk_score"),
        "_batch_id",
        "year", "month", "day",
    )


def build_fraud_features(spark: SparkSession, execution_date: date) -> dict:
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        cfg = yaml.safe_load(f)

    year, month, day = execution_date.year, execution_date.month, execution_date.day
    lookback_date = execution_date - timedelta(days=7)
    staging_path = cfg["tables"]["transactions"]["staging"]

    df = spark.read.parquet(staging_path).filter(
        (F.col("year") * 10000 + F.col("month") * 100 + F.col("day") >=
         lookback_date.year * 10000 + lookback_date.month * 100 + lookback_date.day) &
        (F.col("year") * 10000 + F.col("month") * 100 + F.col("day") <=
         year * 10000 + month * 100 + day)
    ).filter(F.col("is_valid") == True)

    df = _apply_pit_features(df)

    df_features = _select_feature_cols(
        df.filter(
            (F.col("year") == year) &
            (F.col("month") == month) &
            (F.col("day") == day)
        )
    )

    features_path = cfg["lake"]["warehouse"] + "/feat_fraud_features"
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    row_count = df_features.count()
    df_features.repartition(F.col("year"), F.col("month"), F.col("day")) \
        .write.mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("year", "month", "day") \
        .parquet(features_path)

    logger.info("build_fraud_features.done", date=execution_date.isoformat(), row_count=row_count)
    return {"date": execution_date.isoformat(), "row_count": row_count}


def build_fraud_features_full(spark: SparkSession) -> dict:
    """Backfill PIT features for all staging transactions."""
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        cfg = yaml.safe_load(f)

    staging_path = cfg["tables"]["transactions"]["staging"]
    features_path = cfg["lake"]["warehouse"] + "/feat_fraud_features"

    df = spark.read.parquet(staging_path).filter(F.col("is_valid") == True)
    df = _apply_pit_features(df)
    df_features = _select_feature_cols(df)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    row_count = df_features.count()
    df_features.repartition(F.col("year"), F.col("month"), F.col("day")) \
        .write.mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("year", "month", "day") \
        .parquet(features_path)

    logger.info("build_fraud_features_full.done", row_count=row_count)
    return {"row_count": row_count}


if __name__ == "__main__":
    import sys
    from ingestion.spark_session import get_spark_session, stop_spark_session

    spark = get_spark_session("build_fraud_features")
    try:
        if len(sys.argv) > 1:
            result = build_fraud_features(spark, date.fromisoformat(sys.argv[1]))
        else:
            result = build_fraud_features_full(spark)
        print(f"\n=== DONE: {result} ===")
    finally:
        stop_spark_session(spark)
