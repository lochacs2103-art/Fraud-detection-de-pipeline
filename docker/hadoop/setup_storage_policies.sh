#!/bin/bash
# ============================================================
# Setup HDFS Storage Policies — Tiered Storage
# Chạy SAU KHI docker compose up và HDFS đã healthy
# Usage: make hdfs-init (đã include script này)
#
# Storage Policy là gì?
# HDFS cho phép gán policy cho từng directory path.
# Khi Spark/Hive write data vào path đó, HDFS tự động
# chọn đúng loại disk (SSD hay HDD) để lưu blocks.
#
# Policy types:
#   HOT    → lưu trên SSD (nếu có), replicate trên DISK
#   WARM   → lưu trên DISK, 1 replica trên SSD
#   COLD   → lưu trên DISK (archive)
#   ALL_SSD → tất cả replicas trên SSD
# ============================================================

set -e
NAMENODE="namenode"

echo "Waiting for HDFS to be ready..."
until docker exec $NAMENODE hdfs dfs -ls / > /dev/null 2>&1; do
    sleep 2
done
echo "HDFS is ready."

echo ""
echo "=== Creating HDFS directory structure ==="

# ---- Lake directories ----
docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/raw/transactions
docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/raw/users
docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/raw/cards
docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/raw/mcc_codes
docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/raw/fraud_labels

docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/staging/transactions
docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/staging/users
docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/staging/cards
docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/staging/mcc_codes
docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/quarantine/transactions

docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/warehouse/fact_transactions
docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/warehouse/dim_users
docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/warehouse/dim_cards
docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/warehouse/dim_merchants
docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/warehouse/agg_user_daily_stats
docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/warehouse/agg_merchant_risk_score
docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/warehouse/feat_fraud_features
docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/warehouse/transaction_event_log

# Auditable Batch Fraud Intelligence
docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/warehouse/transaction_index
docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/warehouse/fraud_label_history
docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/warehouse/fraud_label_impact_manifest
docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/warehouse/applied_change_ledger
docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/warehouse/daily_fraud_report_snapshots
docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/warehouse/batch_reconciliation_results

docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/warm
docker exec $NAMENODE hdfs dfs -mkdir -p /data/lake/cold

# Hive warehouse directory
docker exec $NAMENODE hdfs dfs -mkdir -p /user/hive/warehouse

# Permissions
docker exec $NAMENODE hdfs dfs -chmod -R 777 /data
docker exec $NAMENODE hdfs dfs -chmod -R 777 /user

echo ""
echo "=== Setting Storage Policies (Tiered Storage) ==="

# ---- HOT policy → SSD volume ----
# raw/ và staging/ → đọc nhiều, cần latency thấp → SSD
docker exec $NAMENODE hdfs storagepolicies -setStoragePolicy \
    -path /data/lake/raw \
    -policy HOT
echo "  [HOT]  /data/lake/raw         → SSD"

docker exec $NAMENODE hdfs storagepolicies -setStoragePolicy \
    -path /data/lake/staging \
    -policy HOT
echo "  [HOT]  /data/lake/staging     → SSD"

docker exec $NAMENODE hdfs storagepolicies -setStoragePolicy \
    -path /data/lake/warehouse \
    -policy HOT
echo "  [HOT]  /data/lake/warehouse   → SSD"

docker exec $NAMENODE hdfs storagepolicies -setStoragePolicy \
    -path /data/lake/quarantine \
    -policy HOT
echo "  [HOT]  /data/lake/quarantine  → SSD"

# ---- WARM policy → HDD volume ----
# warm/ → data 8-90 ngày, ít query hơn → HDD
docker exec $NAMENODE hdfs storagepolicies -setStoragePolicy \
    -path /data/lake/warm \
    -policy WARM
echo "  [WARM] /data/lake/warm        → HDD"

# ---- COLD policy → HDD volume ----
# cold/ → data > 90 ngày, rất ít query → HDD
docker exec $NAMENODE hdfs storagepolicies -setStoragePolicy \
    -path /data/lake/cold \
    -policy COLD
echo "  [COLD] /data/lake/cold        → HDD"

echo ""
echo "=== Verifying Storage Policies ==="
docker exec $NAMENODE hdfs storagepolicies -getStoragePolicy -path /data/lake/raw
docker exec $NAMENODE hdfs storagepolicies -getStoragePolicy -path /data/lake/warm
docker exec $NAMENODE hdfs storagepolicies -getStoragePolicy -path /data/lake/cold

echo ""
echo "=== HDFS Setup Complete ==="
echo ""
echo "Storage layout:"
echo "  SSD volume (de_datanode_ssd):"
echo "    /data/lake/raw/        ← HOT: ingestion output"
echo "    /data/lake/staging/    ← HOT: cleaned + enriched"
echo "    /data/lake/warehouse/  ← HOT: analytics-ready"
echo "    /data/lake/quarantine/ ← HOT: ambiguous records"
echo ""
echo "  HDD volume (de_datanode_hdd):"
echo "    /data/lake/warm/       ← WARM: 8-90 days old"
echo "    /data/lake/cold/       ← COLD: 90+ days old"
echo ""
echo "Access HDFS Web UI: http://localhost:9870"
