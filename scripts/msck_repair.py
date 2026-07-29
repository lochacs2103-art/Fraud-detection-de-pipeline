"""MSCK REPAIR for Hive external tables via Spark (no hive-server/beeline)."""

import os
import sys
from pathlib import Path

ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parent.parent))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ingestion.spark_session import get_spark_session, stop_spark_session

TABLES = [
    "staging.transactions",
    "staging.users",
    "staging.cards",
    "warehouse.feat_fraud_features",
]


def main() -> None:
    spark = get_spark_session("msck_repair")
    try:
        for table in TABLES:
            spark.sql(f"MSCK REPAIR TABLE {table}")
            print(f"MSCK OK: {table}")
        print("=== DONE: msck_repair ===")
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
