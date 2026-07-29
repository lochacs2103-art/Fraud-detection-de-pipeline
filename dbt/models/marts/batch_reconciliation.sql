-- Reconciliation results — unexplained_difference must be 0.

{{ config(materialized='view') }}

SELECT
    event_date,
    knowledge_date,
    raw_count,
    duplicate_count,
    accepted_count,
    quarantined_count,
    unexplained_difference,
    invariant_ok,
    _reconciled_at,
    _batch_id,
    event_year,
    event_month,
    event_day
FROM hive.warehouse.batch_reconciliation_results
