-- create_audit_tables.sql
-- Auditable Batch Fraud Intelligence — Hive external tables (Spark SQL / Hive DDL)
-- Run via:
--   docker exec -e PROJECT_ROOT=/opt/spark/work-dir spark-master \
--     /opt/spark/bin/spark-sql --master local[1] \
--     --conf spark.sql.hive.metastore.version=2.3.9 \
--     --conf spark.sql.hive.metastore.jars=builtin \
--     -f /opt/spark/work-dir/scripts/sql/create_audit_tables.sql
--
-- Create HDFS dirs first: make hdfs-init-audit  (or see setup script)

CREATE DATABASE IF NOT EXISTS warehouse
    LOCATION 'hdfs://namenode:9000/data/lake/warehouse/';

-- Narrow lookup: transaction_id → event partition (selective restatement)
CREATE EXTERNAL TABLE IF NOT EXISTS warehouse.transaction_index (
    transaction_id      STRING,
    user_id             STRING,
    event_date          DATE,
    transaction_date    TIMESTAMP,
    amount              DOUBLE,
    is_fraud            BOOLEAN,
    _indexed_at         TIMESTAMP,
    _batch_id           STRING
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/warehouse/transaction_index/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Full history of known fraud labels (knowledge time)
CREATE EXTERNAL TABLE IF NOT EXISTS warehouse.fraud_label_history (
    transaction_id      STRING,
    is_fraud            BOOLEAN,
    is_fraud_raw        STRING,
    knowledge_date      DATE,
    source_batch_id     STRING,
    _recorded_at        TIMESTAMP
)
PARTITIONED BY (knowledge_year INT, knowledge_month INT, knowledge_day INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/warehouse/fraud_label_history/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Detected late-arriving / changed labels for one knowledge_date run
CREATE EXTERNAL TABLE IF NOT EXISTS warehouse.fraud_label_impact_manifest (
    change_id           STRING,
    transaction_id      STRING,
    event_date          DATE,
    knowledge_date      DATE,
    old_is_fraud        BOOLEAN,
    new_is_fraud        BOOLEAN,
    year                INT,
    month               INT,
    day                 INT,
    change_type         STRING,
    _detected_at        TIMESTAMP,
    _batch_id           STRING
)
PARTITIONED BY (knowledge_year INT, knowledge_month INT, knowledge_day INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/warehouse/fraud_label_impact_manifest/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Idempotent restatement ledger — do not re-apply the same change_id
CREATE EXTERNAL TABLE IF NOT EXISTS warehouse.applied_change_ledger (
    change_id           STRING,
    transaction_id      STRING,
    event_date          DATE,
    knowledge_date      DATE,
    applied_at          TIMESTAMP,
    status              STRING,
    partitions_touched  STRING,
    _batch_id           STRING
)
PARTITIONED BY (applied_year INT, applied_month INT, applied_day INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/warehouse/applied_change_ledger/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Before / after daily fraud report snapshots (as previously reported vs restated)
CREATE EXTERNAL TABLE IF NOT EXISTS warehouse.daily_fraud_report_snapshots (
    report_date         DATE,
    knowledge_date      DATE,
    snapshot_type       STRING,
    change_id           STRING,
    fraud_txn_count     BIGINT,
    fraud_amount_sum    DOUBLE,
    total_txn_count     BIGINT,
    total_amount_sum    DOUBLE,
    _snapshotted_at     TIMESTAMP,
    _batch_id           STRING
)
PARTITIONED BY (report_year INT, report_month INT, report_day INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/warehouse/daily_fraud_report_snapshots/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Source-to-staging reconciliation — unexplained_difference must be 0
CREATE EXTERNAL TABLE IF NOT EXISTS warehouse.batch_reconciliation_results (
    event_date              DATE,
    knowledge_date          DATE,
    raw_count               BIGINT,
    duplicate_count         BIGINT,
    accepted_count          BIGINT,
    quarantined_count       BIGINT,
    unexplained_difference  BIGINT,
    invariant_ok            BOOLEAN,
    _reconciled_at          TIMESTAMP,
    _batch_id               STRING
)
PARTITIONED BY (event_year INT, event_month INT, event_day INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/warehouse/batch_reconciliation_results/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
