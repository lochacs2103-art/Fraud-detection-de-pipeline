-- investigation_queue.sql
-- Explainable queue for analysts: risk score + reason codes.
-- Includes confirmed fraud and high-risk unlabeled / anomalous txns.

{{ config(materialized='table') }}

WITH base AS (
    SELECT
        t.transaction_id,
        t.transaction_date,
        t.user_id,
        t.merchant_id,
        CAST(t.amount AS DOUBLE) AS amount,
        t.is_fraud,
        CAST(f.txn_count_last_1h AS INTEGER)       AS txn_count_last_1h,
        CAST(f.txn_count_last_24h AS INTEGER)      AS txn_count_last_24h,
        CAST(f.amount_vs_user_avg_ratio AS DOUBLE) AS amount_vs_user_avg_ratio,
        f.is_night_txn,
        f.is_weekend,
        f.is_foreign_merchant,
        f.card_on_dark_web,
        CAST(m.fraud_rate AS DOUBLE) AS merchant_fraud_rate,
        m.risk_tier AS merchant_risk_tier,
        t.year,
        t.month,
        t.day
    FROM {{ ref('fact_transactions') }} t
    LEFT JOIN hive.warehouse.feat_fraud_features f
        ON t.transaction_id = f.transaction_id
    LEFT JOIN {{ ref('dim_merchants') }} m
        ON t.merchant_id = m.merchant_id
),
scored AS (
    SELECT
        *,
        -- Heuristic risk 0–100 (not a trained model)
        LEAST(100, GREATEST(0,
            (CASE WHEN is_fraud THEN 40 ELSE 0 END)
          + (CASE WHEN COALESCE(card_on_dark_web, false) THEN 25 ELSE 0 END)
          + (CASE WHEN COALESCE(is_night_txn, false) THEN 10 ELSE 0 END)
          + (CASE WHEN COALESCE(is_foreign_merchant, false) THEN 10 ELSE 0 END)
          + (CASE WHEN COALESCE(txn_count_last_1h, 0) >= 5 THEN 15 ELSE 0 END)
          + (CASE WHEN COALESCE(amount_vs_user_avg_ratio, 0) >= 5 THEN 15 ELSE 0 END)
          + (CASE WHEN COALESCE(merchant_fraud_rate, 0) >= 0.05 THEN 10 ELSE 0 END)
        )) AS risk_score,
        TRIM(BOTH ',' FROM CONCAT_WS(',',
            CASE WHEN is_fraud THEN 'CONFIRMED_FRAUD' END,
            CASE WHEN COALESCE(card_on_dark_web, false) THEN 'CARD_ON_DARK_WEB' END,
            CASE WHEN COALESCE(is_night_txn, false) THEN 'NIGHT_TXN' END,
            CASE WHEN COALESCE(is_weekend, false) THEN 'WEEKEND_TXN' END,
            CASE WHEN COALESCE(is_foreign_merchant, false) THEN 'FOREIGN_MERCHANT' END,
            CASE WHEN COALESCE(txn_count_last_1h, 0) >= 5 THEN 'HIGH_VELOCITY_1H' END,
            CASE WHEN COALESCE(amount_vs_user_avg_ratio, 0) >= 5 THEN 'AMOUNT_ANOMALY' END,
            CASE WHEN COALESCE(merchant_fraud_rate, 0) >= 0.05 THEN 'HIGH_MERCHANT_FRAUD_RATE' END
        )) AS reason_codes
    FROM base
)
SELECT *
FROM scored
WHERE is_fraud = true
   OR risk_score >= 30
   OR reason_codes <> ''
