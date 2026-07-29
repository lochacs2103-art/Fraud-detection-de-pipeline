-- restatement_report.sql
-- Before vs after daily fraud report snapshots for late-label restatements.

{{ config(materialized='view') }}

SELECT
    report_date,
    knowledge_date,
    snapshot_type,
    change_id,
    fraud_txn_count,
    CAST(fraud_amount_sum AS DOUBLE) AS fraud_amount_sum,
    total_txn_count,
    CAST(total_amount_sum AS DOUBLE) AS total_amount_sum,
    _snapshotted_at,
    _batch_id,
    report_year,
    report_month,
    report_day
FROM hive.warehouse.daily_fraud_report_snapshots
