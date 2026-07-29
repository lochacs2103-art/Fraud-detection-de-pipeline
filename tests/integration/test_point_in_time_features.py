"""Point-in-time fraud features must not leak the current transaction."""

from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from transformation.warehouse.build_fraud_features import _apply_pit_features


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_pit_features")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_current_transaction_excluded_from_velocity(spark):
    rows = [
        ("u1", "t1", datetime(2018, 6, 15, 10, 0, 0), 100.0),
        ("u1", "t2", datetime(2018, 6, 15, 10, 30, 0), 200.0),
        ("u1", "t3", datetime(2018, 6, 15, 12, 0, 0), 50.0),
    ]
    df = spark.createDataFrame(
        rows, "user_id string, transaction_id string, transaction_date timestamp, amount double"
    ).withColumn("merchant_state", F.lit("CA"))

    out = _apply_pit_features(df).orderBy("transaction_id").collect()
    by_id = {r.transaction_id: r for r in out}

    # First txn: no prior history
    assert by_id["t1"].txn_count_last_1h == 0
    assert by_id["t1"].amount_sum_last_1h == 0.0
    assert by_id["t1"].user_avg_amount is None

    # t2 is 30min after t1 → sees t1 only (not itself)
    assert by_id["t2"].txn_count_last_1h == 1
    assert float(by_id["t2"].amount_sum_last_1h) == 100.0
    assert float(by_id["t2"].user_avg_amount) == 100.0

    # t3 is 2h after t2 → 1h window empty of priors; 24h sees t1+t2
    assert by_id["t3"].txn_count_last_1h == 0
    assert by_id["t3"].txn_count_last_24h == 2
    assert float(by_id["t3"].amount_sum_last_24h) == 300.0
