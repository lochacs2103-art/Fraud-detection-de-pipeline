"""
Load train_fraud_labels.json vào PostgreSQL source-db.
Dùng Python vì file JSON quá lớn cho pg_read_file() + json_object_keys().

Usage (chạy từ project root):
    python docker/source-db/load_fraud_labels.py
"""

import json
import psycopg2
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
FRAUD_LABELS_PATH = PROJECT_ROOT / "data" / "raw" / "train_fraud_labels.json"

conn = psycopg2.connect(
    host="source-db",
    port=5432,
    dbname="fintech_source",
    user="fintech",
    password="fintech"
)
cur = conn.cursor()

print(f"Loading {FRAUD_LABELS_PATH}...")

with open(FRAUD_LABELS_PATH) as f:
    data = json.load(f)

# Handle both formats:
# Format 1: {"transaction_id": "Yes"/"No", ...}  (flat)
# Format 2: {"target": {"transaction_id": "Yes"/"No", ...}}  (wrapped)
labels = data.get("target", data)
print(f"Total fraud labels: {len(labels)}")

# Batch insert
batch_size = 10000
rows = [(str(k), str(v)) for k, v in labels.items()]

for i in range(0, len(rows), batch_size):
    batch = rows[i:i+batch_size]
    cur.executemany(
        "INSERT INTO raw_fraud_labels (transaction_id, is_fraud) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        batch
    )
    conn.commit()
    print(f"  Inserted {min(i+batch_size, len(rows))}/{len(rows)}")

cur.close()
conn.close()
print("Done.")
