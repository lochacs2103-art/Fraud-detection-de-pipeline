-- Late-label impact visible to analysts (view over Spark audit table).

{{ config(materialized='view') }}

SELECT
    change_id,
    transaction_id,
    event_date,
    knowledge_date,
    old_is_fraud,
    new_is_fraud,
    year,
    month,
    day,
    change_type,
    _detected_at,
    _batch_id,
    knowledge_year,
    knowledge_month,
    knowledge_day
FROM hive.warehouse.fraud_label_impact_manifest
