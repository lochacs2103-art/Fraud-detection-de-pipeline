-- investigation_queue.sql
-- Explainable queue: heuristic risk_score + reason_codes.
-- VIEW (not table): avoids Trino Hive Parquet write failures when
-- feat_fraud_features columns are DECIMAL in metastore but DOUBLE in files.

{{ config(materialized='view') }}

WITH base AS (
    SELECT
        t.transaction_id,
        t.transaction_date,
        t.user_id,
        t.merchant_id,
        CAST(t.amount AS DOUBLE) AS amount,
        t.is_fraud,
        CAST(f.txn_count_last_1h AS INTEGER) AS txn_count_last_1h,
        CAST(f.txn_count_last_24h AS INTEGER) AS txn_count_last_24h,
        -- VARCHAR round-trip forces a true DOUBLE for downstream use
        TRY_CAST(CAST(f.amount_vs_user_avg_ratio AS VARCHAR) AS DOUBLE)
            AS amount_vs_user_avg_ratio,
        COALESCE(f.is_night_txn, false) AS is_night_txn,
        COALESCE(f.is_weekend, false) AS is_weekend,
        COALESCE(f.is_foreign_merchant, false) AS is_foreign_merchant,
        COALESCE(f.card_on_dark_web, false) AS card_on_dark_web,
        TRY_CAST(CAST(COALESCE(m.fraud_rate, 0) AS VARCHAR) AS DOUBLE)
            AS merchant_fraud_rate,
        CAST(m.risk_tier AS VARCHAR) AS merchant_risk_tier,
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
        transaction_id,
        transaction_date,
        user_id,
        merchant_id,
        amount,
        is_fraud,
        txn_count_last_1h,
        txn_count_last_24h,
        amount_vs_user_avg_ratio,
        is_night_txn,
        is_weekend,
        is_foreign_merchant,
        card_on_dark_web,
        merchant_fraud_rate,
        merchant_risk_tier,
        year,
        month,
        day,
        CAST(LEAST(100, GREATEST(0,
              (CASE WHEN is_fraud = true THEN 40 ELSE 0 END)
            + (CASE WHEN card_on_dark_web THEN 25 ELSE 0 END)
            + (CASE WHEN is_night_txn THEN 10 ELSE 0 END)
            + (CASE WHEN is_foreign_merchant THEN 10 ELSE 0 END)
            + (CASE WHEN COALESCE(txn_count_last_1h, 0) >= 5 THEN 15 ELSE 0 END)
            + (CASE WHEN COALESCE(amount_vs_user_avg_ratio, 0) >= 5 THEN 15 ELSE 0 END)
            + (CASE WHEN COALESCE(merchant_fraud_rate, 0) >= 0.05 THEN 10 ELSE 0 END)
        )) AS INTEGER) AS risk_score,
        array_join(
            filter(
                ARRAY[
                    CASE WHEN is_fraud = true THEN 'CONFIRMED_FRAUD' ELSE NULL END,
                    CASE WHEN card_on_dark_web THEN 'CARD_ON_DARK_WEB' ELSE NULL END,
                    CASE WHEN is_night_txn THEN 'NIGHT_TXN' ELSE NULL END,
                    CASE WHEN is_weekend THEN 'WEEKEND_TXN' ELSE NULL END,
                    CASE WHEN is_foreign_merchant THEN 'FOREIGN_MERCHANT' ELSE NULL END,
                    CASE WHEN COALESCE(txn_count_last_1h, 0) >= 5 THEN 'HIGH_VELOCITY_1H' ELSE NULL END,
                    CASE WHEN COALESCE(amount_vs_user_avg_ratio, 0) >= 5 THEN 'AMOUNT_ANOMALY' ELSE NULL END,
                    CASE WHEN COALESCE(merchant_fraud_rate, 0) >= 0.05 THEN 'HIGH_MERCHANT_FRAUD_RATE' ELSE NULL END
                ],
                x -> x IS NOT NULL
            ),
            ','
        ) AS reason_codes
    FROM base
)
SELECT *
FROM scored
WHERE is_fraud = true
   OR risk_score >= 30
   OR length(reason_codes) > 0
