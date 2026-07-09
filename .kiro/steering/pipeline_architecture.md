# Fintech Fraud Detection — Data Engineering Pipeline Architecture

> **Status:** Blueprint v1.1 (synced with implementation)
> **Last Updated:** 2026-06-05
> **Purpose:** Reference architecture cho toàn bộ project. Mọi implementation đều phải align với doc này.

---

## Business Context

MoMo là fintech payment platform. Dataset gồm:
- `transactions_data.csv` — core fact table, lớn nhất
- `users_data.csv` — user dimension
- `cards_data.csv` — card dimension
- `mcc_codes.json` — merchant category lookup (static)
- `train_fraud_labels.json` — ground truth labels cho fraud detection

**Primary use cases:**
1. Fraud detection pipeline (batch)
2. Transaction analytics & reporting
3. User behavior segmentation
4. Merchant risk scoring

---

## Architecture: Lambda + Lakehouse trên HDFS

```
┌──────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                  │
│   CSV / JSON files (transactions, users, cards, mcc, fraud_labels)   │
└───────────────────────┬──────────────────────────────────────────────┘
                        │  COPY (PostgreSQL bulk load)
                        ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     SOURCE DATABASE                                   │
│              PostgreSQL (source-db container)                         │
│   raw_transactions │ raw_users │ raw_cards │ raw_mcc_codes            │
│   raw_fraud_labels                                                    │
│   → giả lập operational DB của MoMo                                  │
└───────────────────────┬──────────────────────────────────────────────┘
                        │  Spark JDBC (parallel read)
                        ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     INGESTION LAYER                                   │
│              Apache Spark — JDBCIngester                              │
│                                                                       │
│  transactions → partition by year/month/day (event_date)             │
│  users        → partition by created_year/created_month              │
│  cards        → partition by card_brand_part/expires_year_part       │
│  mcc_codes    → static, no partition                                 │
│  fraud_labels → no partition                                         │
│                                                                       │
│  + metadata columns: _ingested_at, _source_file, _batch_id           │
└───────────────────────┬──────────────────────────────────────────────┘
                        │  Parquet + Snappy
                        ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        HDFS STORAGE                                   │
│                                                                       │
│  /data/lake/                                                          │
│    ├── raw/        ← exact copy từ source DB, append-only            │
│    ├── staging/    ← cleaned, validated, enriched                    │
│    ├── warehouse/  ← fact, dims, features, event log                 │
│    ├── warm/       ← 8–90 ngày, monthly partition, zstd              │
│    ├── cold/       ← 91+ ngày, quarterly, aggregated only            │
│    └── quarantine/ ← records ambiguous/invalid amount                │
│                                                                       │
│  Catalog: Apache Hive Metastore (external tables)                    │
└──────────┬───────────────────────────────────────────────────────────┘
           │
     ┌─────┴──────┐
     ▼            ▼
┌─────────┐  ┌───────────────────────────────────────────────────────┐
│  Spark  │  │                  SERVING LAYER                         │
│  + dbt  │  │  Trino → query Parquet trên HDFS qua Hive catalog     │
│  (batch │  │  Superset → connect via Trino → dashboards            │
│  transform) └───────────────────────────────────────────────────────┘
└─────────┘
```

---

## Layer 0 — Source Database (PostgreSQL)

**Container:** `source-db` (postgres:14-alpine, port 5432)
**Database:** `momo_source`, user: `momo`

CSV files được COPY vào PostgreSQL khi container khởi động lần đầu qua init scripts:
- `docker/source-db/init/01_schema.sql` — tạo 5 tables (tất cả columns là TEXT)
- `docker/source-db/init/02_load_data.sql` — COPY CSV + flatten JSON → tables

**Tại sao tất cả columns là TEXT trong source DB?**
CSV là nguồn thô, không biết chắc format. Lưu nguyên TEXT, Spark cast về đúng type trong staging. Nếu cast ngay ở DB → mất audit trail, khó debug.

**Tables:**
```
raw_transactions  (id, date, client_id, card_id, amount TEXT, use_chip, merchant_id,
                   merchant_city, merchant_state, zip, mcc, errors,
                   _loaded_at, _source_file)

raw_users         (id, current_age, retirement_age, birth_year, birth_month,
                   gender, address, latitude, longitude,
                   per_capita_income, yearly_income, total_debt,
                   credit_score, num_credit_cards, _loaded_at, _source_file)

raw_cards         (id, client_id, card_brand, card_type, card_number TEXT,
                   expires, cvv TEXT, has_chip, num_cards_issued, credit_limit,
                   acct_open_date, year_pin_last_changed, card_on_dark_web,
                   _loaded_at, _source_file)
                   ⚠ card_number và cvv bị MASK/DROP trong staging — PCI DSS

raw_mcc_codes     (mcc_code, description, _loaded_at, _source_file)

raw_fraud_labels  (transaction_id, is_fraud TEXT, _loaded_at, _source_file)
```

---

## Layer 1 — RAW (HDFS)

**Công cụ:** Apache Spark, đọc từ source DB qua JDBC
**Format:** Parquet + Snappy compression
**Mode:** Append-only — không bao giờ overwrite hay delete raw data

### JDBC Parallel Read

Vấn đề: JDBC single-threaded = 1 connection = chậm với bảng lớn.
Giải pháp: Spark chia bảng thành N partitions theo numeric range, mỗi partition = 1 connection song song.

```
num_partitions=8, lower=1, upper=1000000:

Task 1: SELECT * FROM raw_transactions WHERE id >= 1       AND id < 125001
Task 2: SELECT * FROM raw_transactions WHERE id >= 125001  AND id < 250001
...
Task 8: SELECT * FROM raw_transactions WHERE id >= 875001  AND id <= 1000000
```

Cấu hình trong `config/source_db.yaml`:
```yaml
parallel_read:
  transactions:
    partition_column: "id"
    num_partitions: 8
  users:
    partition_column: "id"
    num_partitions: 4
  mcc_codes:
    num_partitions: 1   # nhỏ, single partition
fetch_size: 10000       # rows per fetch, default 10 là quá nhỏ
```

**Lưu ý:** `id` trong PostgreSQL là TEXT → dùng subquery để CAST sang BIGINT trong JDBC options vì Spark JDBC partition column phải là NUMERIC.

### Partition Strategy

| Table | Partition Keys | Lý do |
|---|---|---|
| transactions | `year`, `month`, `day` | Use case chính: query fraud theo time range |
| users | `created_year`, `created_month` | Cohort analysis theo signup/birth time |
| cards | `card_brand_part`, `expires_year_part` | Fraud cluster theo card type |
| mcc_codes | — | ~300 rows, partition overhead > benefit |
| fraud_labels | — | Nhỏ, join vào transactions trong staging |

**HDFS paths:**
```
/data/lake/raw/transactions/year=2023/month=06/day=15/part-00000.parquet
/data/lake/raw/users/created_year=1990/created_month=06/part-00000.parquet
/data/lake/raw/cards/card_brand_part=visa/expires_year_part=2025/part-00000.parquet
/data/lake/raw/mcc_codes/part-00000.parquet
/data/lake/raw/fraud_labels/part-00000.parquet
```

### Metadata columns (thêm vào mỗi record khi ingest)
```
_ingested_at  TIMESTAMP  — lúc job chạy (ingestion time, khác event time)
_source_file  STRING     — tên table nguồn (raw_transactions, ...)
_batch_id     STRING     — UUID của batch run, dùng để idempotency check
```

### Code structure
```
ingestion/
  base_ingester.py        — Abstract base, Template Method pattern
  jdbc_ingester.py        — JDBCIngester base + 5 concrete classes:
                              TransactionJDBCIngester  (partition: year/month/day)
                              UserJDBCIngester         (partition: created_year/month)
                              CardJDBCIngester         (partition: card_brand/expires_year)
                              MCCJDBCIngester          (no partition)
                              FraudLabelJDBCIngester   (no partition)
  spark_session.py        — SparkSessionFactory singleton
  schema/
    transactions_schema.py  — RAW_CSV_SCHEMA + STAGING_SCHEMA + PARTITION_COLS
    users_schema.py
    cards_schema.py
    mcc_schema.py
```

---

## Layer 2 — STAGING (HDFS + Hive External Tables)

**Công cụ:** Apache Spark jobs đọc từ RAW layer
**Output:** Parquet trên HDFS + Hive External Tables để Trino query được

### Transformations theo thứ tự

**1. Schema enforcement & type casting**
Tất cả columns đang là STRING (từ raw) → cast về đúng type:
- `mcc` → INT
- `transaction_date` → TIMESTAMP với format `yyyy-MM-dd HH:mm:ss`
- `is_fraud` → BOOLEAN (từ "0"/"1")
- `credit_limit`, income fields → DOUBLE (sau khi strip "$")

**2. Amount Cleaning** ← phức tạp nhất, xem section riêng bên dưới

**3. PII/PCI Masking**
```
card_number → card_number_masked: "XXXX-XXXX-XXXX-1234" (chỉ giữ 4 số cuối)
cvv → DROP hoàn toàn, không lưu vào bất kỳ layer nào
```

**4. Deduplication**
```python
df.dropDuplicates(["transaction_id"])  # dedup key
```

**5. Enrichment (broadcast joins)**
```python
# mcc_codes ~300 rows → broadcast, không shuffle
transactions.join(broadcast(mcc_df), on="mcc", how="left")
    → thêm mcc_description

# cards dim → broadcast
transactions.join(broadcast(cards_df.select("card_id","card_brand","card_type")), ...)
    → thêm card_brand, card_type

# fraud labels → broadcast (nhỏ)
transactions.join(broadcast(fraud_df), on="transaction_id", how="left")
    → thêm is_fraud
```

**6. Data Quality flag**
```
is_valid = True  nếu: transaction_id NOT NULL
                      AND amount NOT NULL (đã parse được)
                      AND transaction_date NOT NULL
                      AND user_id NOT NULL
```

### Hive External Table DDL

```sql
-- Database
CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS staging;
CREATE DATABASE IF NOT EXISTS warehouse;

-- Staging transactions (full schema sau enrichment)
CREATE EXTERNAL TABLE staging.transactions (
    transaction_id    STRING,
    transaction_date  TIMESTAMP,
    user_id           STRING,
    card_id           STRING,
    -- Amount: 5 cột thay vì 1 (xem Amount Cleaning section)
    amount_raw        STRING,
    amount            DOUBLE,
    amount_currency   STRING,
    amount_format     STRING,
    amount_parse_note STRING,
    -- Enriched
    use_chip          STRING,
    merchant_id       STRING,
    merchant_city     STRING,
    merchant_state    STRING,
    zip               STRING,
    mcc               INT,
    mcc_description   STRING,
    card_brand        STRING,
    card_type         STRING,
    card_number_masked STRING,
    errors            STRING,
    is_fraud          BOOLEAN,
    is_valid          BOOLEAN,
    -- Metadata
    _ingested_at      TIMESTAMP,
    _source_file      STRING,
    _batch_id         STRING
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/staging/transactions/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Staging users
CREATE EXTERNAL TABLE staging.users (
    user_id             STRING,
    current_age         INT,
    retirement_age      INT,
    birth_year          INT,
    birth_month         INT,
    gender              STRING,
    address             STRING,
    latitude            DOUBLE,
    longitude           DOUBLE,
    per_capita_income   DOUBLE,
    yearly_income       DOUBLE,
    total_debt          DOUBLE,
    credit_score        INT,
    num_credit_cards    INT,
    is_valid            BOOLEAN,
    _ingested_at        TIMESTAMP,
    _source_file        STRING,
    _batch_id           STRING
)
PARTITIONED BY (created_year INT, created_month INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/staging/users/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Staging cards (PAN masked, CVV dropped)
CREATE EXTERNAL TABLE staging.cards (
    card_id               STRING,
    user_id               STRING,
    card_brand            STRING,
    card_type             STRING,
    card_number_masked    STRING,   -- "XXXX-XXXX-XXXX-1234"
    -- cvv: KHÔNG có, dropped hoàn toàn
    expires_month         INT,
    expires_year          INT,
    has_chip              BOOLEAN,
    num_cards_issued      INT,
    credit_limit          DOUBLE,
    acct_open_date        STRING,
    year_pin_last_changed INT,
    card_on_dark_web      BOOLEAN,
    is_valid              BOOLEAN,
    _ingested_at          TIMESTAMP,
    _source_file          STRING,
    _batch_id             STRING
)
PARTITIONED BY (card_brand_part STRING, expires_year_part INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/staging/cards/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Staging mcc_codes (no partition)
CREATE EXTERNAL TABLE staging.mcc_codes (
    mcc             INT,
    mcc_code        STRING,
    mcc_description STRING,
    _ingested_at    TIMESTAMP,
    _source_file    STRING,
    _batch_id       STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/staging/mcc_codes/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Quarantine: records có amount AMBIGUOUS/INVALID
CREATE EXTERNAL TABLE staging.transactions_quarantine (
    transaction_id    STRING,
    amount_raw        STRING,
    amount_format     STRING,
    amount_parse_note STRING,
    quarantine_reason STRING,
    quarantine_ts     TIMESTAMP,
    resolved_by       STRING,
    resolved_at       TIMESTAMP,
    is_resolved       BOOLEAN,
    _batch_id         STRING
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/quarantine/transactions/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
```

---

## Layer 3 — WAREHOUSE (HDFS + Hive + dbt)

**Công cụ:** dbt-trino chạy SQL models, đọc từ staging, output ra warehouse tables
**dbt connect:** Trino → Hive Metastore → HDFS

### Fact Table

```sql
-- warehouse.fact_transactions
-- Partition: year, month (ít granular hơn staging — warehouse query thường theo month)
-- Bucketing: user_id, 32 buckets → tối ưu join với dim_users
CREATE EXTERNAL TABLE warehouse.fact_transactions (
    transaction_id      STRING,
    transaction_date    TIMESTAMP,
    user_id             STRING,
    card_id             STRING,
    merchant_id         STRING,
    amount              DOUBLE,
    amount_currency     STRING,
    mcc                 INT,
    mcc_description     STRING,
    card_brand          STRING,
    card_type           STRING,
    use_chip            STRING,
    merchant_city       STRING,
    merchant_state      STRING,
    is_fraud            BOOLEAN,
    is_valid            BOOLEAN,
    _batch_id           STRING
)
PARTITIONED BY (year INT, month INT)
CLUSTERED BY (user_id) INTO 32 BUCKETS
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/warehouse/fact_transactions/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
```

### Dimension Tables

```sql
-- warehouse.dim_users — SCD Type 2
-- Giữ lịch sử thay đổi: user thay đổi địa chỉ/income → row mới, row cũ có valid_to
CREATE EXTERNAL TABLE warehouse.dim_users (
    user_sk             BIGINT,    -- surrogate key (auto-increment)
    user_id             STRING,    -- natural key
    current_age         INT,
    retirement_age      INT,
    gender              STRING,
    address             STRING,
    latitude            DOUBLE,
    longitude           DOUBLE,
    per_capita_income   DOUBLE,
    yearly_income       DOUBLE,
    total_debt          DOUBLE,
    credit_score        INT,
    num_credit_cards    INT,
    -- SCD Type 2 columns
    valid_from          TIMESTAMP,
    valid_to            TIMESTAMP,  -- 9999-12-31 nếu là row hiện tại
    is_current          BOOLEAN,
    _batch_id           STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/warehouse/dim_users/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- warehouse.dim_cards — SCD Type 1 (overwrite)
CREATE EXTERNAL TABLE warehouse.dim_cards (
    card_id               STRING,
    user_id               STRING,
    card_brand            STRING,
    card_type             STRING,
    card_number_masked    STRING,
    expires_month         INT,
    expires_year          INT,
    has_chip              BOOLEAN,
    credit_limit          DOUBLE,
    card_on_dark_web      BOOLEAN,
    _batch_id             STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/warehouse/dim_cards/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- warehouse.dim_merchants — derived từ transactions + mcc_codes
CREATE EXTERNAL TABLE warehouse.dim_merchants (
    merchant_id       STRING,
    merchant_city     STRING,
    merchant_state    STRING,
    mcc               INT,
    mcc_description   STRING,
    fraud_rate        DOUBLE,   -- tính từ historical data
    total_txn_count   BIGINT,
    _updated_at       TIMESTAMP
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/warehouse/dim_merchants/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
```

### Aggregate Tables

```sql
-- warehouse.agg_user_daily_stats
CREATE EXTERNAL TABLE warehouse.agg_user_daily_stats (
    user_id             STRING,
    txn_date            DATE,
    total_txn_count     INT,
    total_amount        DOUBLE,
    avg_amount          DOUBLE,
    max_amount          DOUBLE,
    fraud_txn_count     INT,
    distinct_merchants  INT
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/warehouse/agg_user_daily_stats/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- warehouse.agg_merchant_risk_score
CREATE EXTERNAL TABLE warehouse.agg_merchant_risk_score (
    merchant_id         STRING,
    mcc_description     STRING,
    total_txn_count     BIGINT,
    fraud_txn_count     BIGINT,
    fraud_rate          DOUBLE,
    avg_amount          DOUBLE,
    risk_tier           STRING    -- LOW | MEDIUM | HIGH | CRITICAL
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/warehouse/agg_merchant_risk_score/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
```

### Feature Store

```sql
-- warehouse.feat_fraud_features — feature store cho ML team
CREATE EXTERNAL TABLE warehouse.feat_fraud_features (
    transaction_id              STRING,
    user_id                     STRING,
    -- Velocity features
    txn_count_last_1h           INT,
    txn_count_last_24h          INT,
    txn_count_last_7d           INT,
    amount_sum_last_1h          DOUBLE,
    amount_sum_last_24h         DOUBLE,
    -- Amount anomaly
    amount_vs_user_avg_ratio    DOUBLE,    -- amount / avg(amount) of this user
    amount_vs_merchant_avg_ratio DOUBLE,
    -- Geo
    is_foreign_merchant         BOOLEAN,
    -- Time
    is_weekend                  BOOLEAN,
    is_night_txn                BOOLEAN,   -- 0h–5h
    -- Card risk
    card_on_dark_web            BOOLEAN,
    -- Label
    is_fraud                    BOOLEAN,
    risk_score                  DOUBLE,    -- model output (nếu có)
    _batch_id                   STRING
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/warehouse/feat_fraud_features/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
```

### Transaction Event Log

```sql
-- warehouse.transaction_event_log — append-only audit trail
-- Log từng state transition của giao dịch
-- KHÔNG bao giờ update/delete
CREATE EXTERNAL TABLE warehouse.transaction_event_log (
    transaction_id  STRING,
    event_type      STRING,    -- INITIATED | FRAUD_CHECK_PASS | FRAUD_CHECK_FAIL
                               -- BANK_APPROVED | BANK_DECLINED | COMPLETED | REVERSED
    event_time      TIMESTAMP,
    event_payload   STRING,    -- JSON string: {score: 0.12, threshold: 0.7, ...}
    _ingested_at    TIMESTAMP,
    _batch_id       STRING
)
PARTITIONED BY (event_year INT, event_month INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/warehouse/transaction_event_log/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
```

### dbt Models Structure

```
dbt/
  profiles.yml              → connect Trino: host=trino, port=8080, catalog=hive
  dbt_project.yml
  models/
    staging/
      stg_transactions.sql  → select + rename columns từ staging.transactions
      stg_users.sql
      stg_cards.sql
    warehouse/
      fact_transactions.sql → incremental, unique_key=transaction_id
      dim_users.sql         → SCD Type 2 logic
      dim_cards.sql         → SCD Type 1
      dim_merchants.sql     → derived
    marts/
      fraud_features.sql    → window functions cho velocity features
      user_daily_stats.sql  → GROUP BY user, date
      merchant_risk_score.sql
  tests/
    assert_no_null_transaction_id.sql
    assert_fraud_label_coverage.sql
```

**dbt incremental strategy:**
```sql
-- fact_transactions.sql
{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge',
    partition_by={'field': 'year', 'data_type': 'int'}
) }}
SELECT ... FROM {{ ref('stg_transactions') }}
{% if is_incremental() %}
WHERE year >= {{ var('start_year') }} AND month >= {{ var('start_month') }}
{% endif %}
```

---

## Transaction Tracing

Mọi giao dịch trace đầy đủ qua join keys:

```
user_id ──► dim_users            (SCD Type 2, lấy is_current=true)
   │
   └──► fact_transactions ──────► transaction_id
              │                        │
              ├──► dim_cards           └──► transaction_event_log
              │    (card_brand,              (audit: INITIATED → COMPLETED)
              │     card_type,
              │     card_on_dark_web)
              │
              ├──► dim_merchants
              │    (merchant_name, mcc, fraud_rate)
              │
              └──► feat_fraud_features
                   (is_fraud, risk_score,
                    velocity features)
```

**Query trace từ user_id:**
```sql
SELECT
    t.transaction_id, t.amount, t.transaction_date,
    u.current_age, u.gender, u.yearly_income,
    c.card_brand, c.card_type, c.card_number_masked, c.card_on_dark_web,
    m.merchant_city, m.mcc_description, m.fraud_rate AS merchant_fraud_rate,
    f.is_fraud, f.risk_score, f.txn_count_last_24h, f.amount_vs_user_avg_ratio
FROM warehouse.fact_transactions t
JOIN warehouse.dim_users u
    ON t.user_id = u.user_id AND u.is_current = true
JOIN warehouse.dim_cards c
    ON t.card_id = c.card_id
JOIN warehouse.dim_merchants m
    ON t.merchant_id = m.merchant_id
LEFT JOIN warehouse.feat_fraud_features f
    ON t.transaction_id = f.transaction_id
WHERE t.user_id = 'USR_12345'
  AND t.year = 2023        -- partition pruning
  AND t.month = 6;         -- partition pruning
```

---

## Amount Field Cleaning Strategy

### Vấn đề
Amount đến ở nhiều format tùy source system và locale:
```
"$1,234.56"   → US: phẩy=thousand, chấm=decimal     → 1234.56  ✓ PARSE OK
"1.234,56"    → EU: chấm=thousand, phẩy=decimal      → 1234.56  ✓ PARSE OK
"₫200,000"    → VND: phẩy=thousand, không decimal    → 200000.0 ✓ PARSE OK
"(500.00)"    → accounting negative                   → -500.0   ✓ PARSE OK
"1,234"       → US=1234 hay EU=1.234?                → NULL     ✗ AMBIGUOUS
"1.234"       → EU=1234 hay decimal=1.234?           → NULL     ✗ AMBIGUOUS
"N/A"         → placeholder                          → NULL     ✗ INVALID
```

### Nguyên tắc: Detect → Classify → Parse → Validate → Flag
**Không bao giờ tự động sửa khi ambiguous. Không bao giờ drop record.**

### Logic detect format
```
Có cả dot VÀ comma:
  → rfind('.') > rfind(',')  →  chấm ở sau = decimal separator  → US format
  → rfind(',') > rfind('.')  →  phẩy ở sau = decimal separator  → EU format

Chỉ có 1 loại separator, phần sau có đúng 3 chữ số:
  → Nếu currency = VND  → đó là thousand separator  → VN format
  → Nếu không có context → AMBIGUOUS → quarantine

Không có separator nào:
  → CLEAN numeric
```

### Output schema trong staging (5 cột thay vì 1)
```
amount_raw        STRING  — giữ nguyên raw, không bao giờ xóa
amount            DOUBLE  — parsed value, NULL nếu không parse được
amount_currency   STRING  — "USD" | "VND" | "EUR" | NULL
amount_format     STRING  — "US" | "EU" | "CLEAN" | "VN" | "AMBIGUOUS" | "INVALID"
amount_parse_note STRING  — lý do: "ambiguous: could be 1234 or 1.234"
```

### Quarantine flow
```
AMBIGUOUS/INVALID record
    │
    ▼
staging.transactions_quarantine
    │
    ├──► Rule-based auto-resolution
    │       (currency=VND → thousand separator → resolve)
    │       (merchant avg amount >> 1000 → 1,234 = 1234 → resolve)
    │       ↓ resolved → back to staging
    │
    └──► Manual review queue (Airflow task)
            ↓ human confirm via resolved_by / resolved_at
            ↓ re-process vào staging
```

### Implementation
```
transformation/staging/
  amount_parser.py        — AmountParser class, AmountFormat enum
  clean_transactions.py   — apply AmountParser, PCI masking, dedup
  enrich_transactions.py  — broadcast joins với mcc, cards, fraud_labels
```

---

## Orchestration — Apache Airflow

**DAG: `fraud_data_pipeline`**
```
Schedule: @daily (02:00 AM)
Catchup: True — có thể backfill từ ngày đầu dataset

Task dependency graph:

ingest_transactions  ──┐
ingest_users         ──┤
ingest_cards         ──┤──► validate_raw (Great Expectations)
ingest_mcc           ──┤          │
ingest_fraud_labels  ──┘          ▼
                            spark_raw_to_staging
                            (chỉ process đúng 1 ngày = execution_date)
                                  │
                                  ▼
                            hive_msck_repair_partitions  ← sync partitions vào Metastore
                                  │                        tránh Spark list HDFS
                                  ▼
                            dbt_run_staging_models
                                  │
                                  ▼
                            dbt_run_warehouse_models
                                  │
                                  ▼
                            dbt_test_data_quality
                                  │
                            ┌─────┴─────┐
                            ▼           ▼
                      notify_success  notify_failure
                                        │
                                        ▼
                                  flag_quarantine_records
```

**DAG: `compaction_pipeline`**
```
Schedule: @daily (03:00 AM, sau fraud_data_pipeline)

compact_transactions_yesterday
compact_users_last_month        (chỉ chạy ngày đầu tháng)
compact_cards_last_month        (chỉ chạy ngày đầu tháng)
vacuum_quarantine_resolved      (xóa records resolved > 30 ngày)
```

**DAG: `backfill_pipeline`** ← triggered manually khi cần reprocess data cũ
```
Schedule: None (manual trigger với date range params)
max_active_runs: 4  ← 4 tháng xử lý song song, không chạy cả năm 1 lúc

Params:
  start_date: "2020-01-01"
  end_date:   "2023-12-31"

Strategy: chia thành monthly tasks, mỗi run = 1 tháng
  → Không bao giờ 1 Spark job phải xử lý cả năm
  → 4 concurrent runs = 4 tháng song song
```

### Partition Granularity Strategy

Đây là trade-off quan trọng:

```
Part quá nhỏ (theo ngày) → quá nhiều HDFS partitions
  → Spark tạo 1 RDD task per folder → 1095 tasks nhỏ → driver OOM

Part quá lớn (theo năm) → quá ít partitions
  → Không parallel được → chờ lâu
```

**Giải pháp: Tách biệt HDFS partition (cho filter) và Spark RDD partition (cho parallel)**

```python
# SAI: để Spark tự chia theo HDFS folders
df = spark.read.parquet("hdfs://.../transactions/")  # 1095 tasks

# ĐÚNG: đọc đúng range cần → coalesce về số hợp lý
df = spark.read.parquet("hdfs://.../transactions/") \
    .filter((F.col("year") == 2023) & (F.col("month") == 6)) \
    .coalesce(50)   # 50 RDD partitions, mỗi cái ~128MB

# TỐT NHẤT: Airflow @daily → mỗi run chỉ đọc 1 ngày
input_path = f"hdfs://.../year={year}/month={month}/day={day}/"
df = spark.read.parquet(input_path)
# Data 1 ngày → vài chục RDD partitions tự nhiên, không cần tune gì
```

**MSCK REPAIR sau mỗi ingestion** — tránh Spark list HDFS:
```sql
-- Spark hỏi Hive Metastore "partition này ở đâu?" thay vì scan HDFS folder
-- Nhanh hơn nhiều khi có hàng nghìn partitions
MSCK REPAIR TABLE raw.transactions;
MSCK REPAIR TABLE staging.transactions;
```

---

## Serving Layer — Trino

Trino connect vào Hive Metastore, query Parquet trực tiếp trên HDFS. Không copy data.

```
Catalogs:
  hive        → HDFS (raw, staging, warehouse, warm, cold)
  postgresql  → source-db (federated query nếu cần)

Query path:
  Superset → SQLAlchemy → Trino:8080 → Hive Metastore:9083 → HDFS:9000 → Parquet
```

---

## Visualization — Apache Superset

Connection string: `trino://trino@trino:8080/hive`

Dashboards:
1. **Fraud Overview** — fraud rate theo ngày/MCC/card brand
2. **Transaction Analytics** — volume, amount distribution, top merchants
3. **User Risk Profile** — high-risk users, velocity anomalies
4. **Pipeline Health** — data freshness, row counts per partition, quarantine count

---

## Storage Lifecycle & Fragmentation Prevention

### External Fragmentation — Small Files trên HDFS

**Nguyên nhân:** late arriving data, micro-batch ingestion, reprocessing, Spark parallelism cao.
**Hậu quả:** HDFS NameNode lưu metadata từng file trong RAM → OOM → cả cluster chết.

**Giải pháp: Compaction Job** (`transformation/compaction/compactor.py`)
```python
# Đọc toàn bộ partition → merge → ghi lại, dedup trong quá trình
# Target: 128MB per file
# Mode: dynamic partition overwrite → chỉ overwrite đúng partition đang compact
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

df.dropDuplicates(["transaction_id"]) \
  .coalesce(target_files) \
  .write.mode("overwrite") \
  .option("compression", "snappy") \
  .parquet(partition_path)
```
Rule: không bao giờ để 1 partition có > 1000 files.

### Internal Fragmentation — Parquet Row Group Layout

**Vấn đề:** data không sort → Row Group statistics range rộng → predicate pushdown vô dụng.
Filter `amount > 100000` vẫn phải đọc hết nếu row group min=1, max=999999.

**Giải pháp: sortWithinPartitions**
```python
# Sort trong partition, KHÔNG gây global shuffle (khác với .sort())
df.sortWithinPartitions(
    F.col("user_id"),    # primary: group same user → query by user đọc ít row groups
    F.col("year"),
    F.col("month"),
    F.col("day")
)
```

Row Group config:
```python
spark.conf.set("spark.sql.parquet.blockSize", str(128 * 1024 * 1024))  # 128MB
spark.conf.set("spark.sql.parquet.pageSize",  str(1   * 1024 * 1024))  # 1MB page
spark.conf.set("spark.sql.parquet.dictionaryEncodingEnabled", "true")  # tốt cho card_brand, mcc
```

### Tiered Storage — Hot / Warm / Cold

| Tier | Thời gian | Storage | Partition | Compression | File size |
|------|-----------|---------|-----------|-------------|-----------|
| HOT  | 0–7 ngày  | HDFS SSD | day  | Snappy | 128MB |
| WARM | 8–90 ngày | HDFS HDD | month | ZSTD  | 256MB |
| COLD | 91+ ngày  | HDFS HDD | quarter | ZSTD max | 256MB (aggregated only) |

**Monthly tiering job:** merge 30 daily partitions → 1 monthly, đổi compression → zstd, verify success → xóa daily partitions.

**Storage lifecycle:**
```
Ingest → raw/ (nhiều files nhỏ)
           ↓ Daily Compaction @ 03:00 AM
         raw/ (files ~128MB, sorted by user_id, deduped)
           ↓ Monthly Tiering (ngày 1 mỗi tháng)
         warm/ (files ~256MB, zstd, partition by month)
           ↓ Quarterly Archive (sau 90 ngày)
         cold/ (aggregated only, zstd max)
```

---

## Spark Optimization Strategy

**Core principle:** đọc ít → shuffle ít → ghi đúng format.

### 1. Partition Pruning
Filter trên partition columns **trước tất cả mọi thứ** → Spark chỉ mở đúng folder HDFS.
```python
# ALWAYS first
df.filter((F.col("year") == 2023) & (F.col("month") == 6) & (F.col("day") == 15))
```
Với dataset 3 năm (1095 ngày), query 1 ngày → chỉ đọc 1/1095 data.

### 2. Predicate Pushdown
Parquet lưu min/max statistics cho mỗi Row Group trong footer.
Spark đọc footer trước → skip Row Group nếu giá trị ngoài range → không load vào memory.
```python
spark.conf.set("spark.sql.parquet.filterPushdown", "true")       # default true
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")
```

### 3. Broadcast Join — bắt buộc cho dimension tables
```python
# sort-merge join (default) → shuffle cả 2 sides qua network = chậm
# broadcast join → copy bảng nhỏ lên mỗi executor = không shuffle = nhanh 10-50x

from pyspark.sql.functions import broadcast

transactions_df.join(broadcast(mcc_df), on="mcc", how="left")       # mcc ~300 rows
transactions_df.join(broadcast(fraud_df), on="transaction_id", how="left")
```
Config: `spark.sql.autoBroadcastJoinThreshold = 52428800` (50MB) — Spark tự broadcast nếu bảng < 50MB.

### 4. Adaptive Query Execution (AQE) — Spark 3.0+, luôn bật
AQE re-optimize query plan ở **runtime** dựa trên actual data stats, không phải estimates.
```python
spark.conf.set("spark.sql.adaptive.enabled",                        "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",     "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",               "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes",   "134217728")  # 128MB
```
AQE làm được:
- Tự coalesce shuffle partitions nhỏ → giảm task overhead
- Tự convert sang broadcast join nếu runtime size < threshold
- Tự split skewed partitions thành sub-partitions

### 5. Data Skew — Salting khi AQE không đủ
Skew: merchant "Grab" có 10M transactions, median merchant có 1000. Executor xử lý "Grab" là straggler → toàn bộ job wait.
```python
# AQE xử lý tự động (ưu tiên):
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
# Partition bị coi là skewed nếu size > 5x median

# Manual salting nếu cần:
SALT = 10
txn_salted = txn_df.withColumn(
    "merchant_id_salted",
    F.concat(F.col("merchant_id"), F.lit("_"), (F.rand() * SALT).cast("int").cast("string"))
)
merchant_exploded = merchant_df.withColumn("salt", F.explode(F.array([F.lit(i) for i in range(SALT)]))) \
    .withColumn("merchant_id_salted", F.concat(F.col("merchant_id"), F.lit("_"), F.col("salt").cast("string")))
result = txn_salted.join(merchant_exploded, on="merchant_id_salted").drop("merchant_id_salted", "salt")
```

### 6. Columnar Pruning
```python
# Parquet columnar: chỉ đọc cột cần thiết
# select() NGAY SAU KHI ĐỌC — trước join, trước filter
df = spark.read.parquet(path).select("transaction_id", "user_id", "amount", "year", "month")
# Không đọc 15 cột không cần vào memory
```

### 7. File Output — tránh small file problem
```python
# Target: 1 file = 1 HDFS block = 128MB
estimated_size_gb = 5.0
target_files = max(1, int(estimated_size_gb / 0.128))

# repartition theo partition columns → data cùng partition về cùng executor → ít files hơn
df.repartition(*[F.col(c) for c in ["year", "month", "day"]]) \
  .write \
  .mode("overwrite") \
  .option("compression", "snappy") \
  .partitionBy("year", "month", "day") \
  .parquet(output_path)

# coalesce vs repartition:
# coalesce(n)    → giảm partition, KHÔNG shuffle, dùng khi chỉ cần giảm số files
# repartition(n) → tăng/giảm, CÓ shuffle, dùng khi cần even distribution
```

### 8. Caching — dùng đúng chỗ
```python
# CHỈ cache DataFrame dùng nhiều lần trong cùng 1 job
mcc_df.cache()
mcc_df.count()   # trigger materialization ngay — lazy cache không có ích

# KHÔNG cache DataFrame lớn (transactions)
# Luôn unpersist sau khi xong việc
mcc_df.unpersist()

# Storage level phù hợp:
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK_SER)  # spill to disk nếu memory không đủ
```

### 9. JDBC Parallel Read (ingestion từ source DB)
```python
# Tính bounds trước (1 query nhỏ)
bounds = spark.read.jdbc(url, f"(SELECT MIN(id), MAX(id) FROM {table}) t").collect()[0]

# Parallel read với N partitions
df = spark.read.jdbc(
    url=jdbc_url,
    table=table,
    column="id_numeric",    # phải là NUMERIC
    lowerBound=bounds[0],
    upperBound=bounds[1],
    numPartitions=8,        # 8 connections song song
    properties={"fetchsize": "10000"}
)
```

### 10. Executor Config chuẩn
```python
SPARK_CONFIG = {
    # --- Executor sizing ---
    # Tổng OS memory per executor = executor.memory + memoryOverhead
    "spark.executor.memory":         "4g",   # JVM heap
    "spark.executor.memoryOverhead": "1g",   # off-heap: Python worker, JVM native, network buffers
    # ⚠ memoryOverhead hay bị quên → executor bị OS kill vì OOM
    # Tổng = 5GB per executor

    "spark.driver.memory":           "2g",
    "spark.driver.memoryOverhead":   "512m",
    "spark.executor.cores":          "5",    # sweet spot: HDFS cho 3 connections/executor

    # --- Memory fraction (Unified Memory Manager) ---
    # Executor JVM heap = Reserved(300MB hardcoded) + Usable(phần còn lại)
    # Usable chia 2:
    #   Unified Pool = usable × memory.fraction  → cho Storage + Execution
    #   User Memory  = usable × (1 - fraction)   → Python UDFs, user data
    "spark.memory.fraction":         "0.6",  # 60% usable → Unified Pool
    # Trong Unified Pool, Storage và Execution có thể mượn nhau
    # Job này ít cache (chỉ mcc_codes nhỏ) → storageFraction thấp
    # → Execution có thêm room cho shuffle/join/agg
    "spark.memory.storageFraction":  "0.3",  # 30% Unified → Storage, 70% → Execution

    # Off-heap Spark (giảm GC pressure cho large datasets)
    "spark.memory.offHeap.enabled":  "true",
    "spark.memory.offHeap.size":     "1g",

    # --- AQE ---
    "spark.sql.adaptive.enabled":               "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled":      "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "134217728",  # 128MB

    # --- Broadcast ---
    "spark.sql.autoBroadcastJoinThreshold": "52428800",  # 50MB

    # --- Dynamic partition overwrite ---
    "spark.sql.sources.partitionOverwriteMode": "dynamic",

    # --- Dynamic allocation ---
    "spark.dynamicAllocation.enabled":          "true",
    "spark.dynamicAllocation.minExecutors":     "2",
    "spark.dynamicAllocation.maxExecutors":     "20",
    "spark.dynamicAllocation.initialExecutors": "5",

    # --- Speculation: re-launch straggler tasks ---
    "spark.speculation":              "true",
    "spark.speculation.multiplier":   "1.5",  # task chậm hơn 1.5x median → re-launch

    # --- Serializer ---
    "spark.serializer":  "org.apache.spark.serializer.KryoSerializer",
    "spark.kryo.unsafe": "true",

    # --- Parquet ---
    "spark.sql.parquet.filterPushdown":         "true",
    "spark.sql.parquet.enableVectorizedReader": "true",
    "spark.sql.parquet.compression.codec":      "snappy",
}
```

### Executor Memory Layout (visualized)
```
┌─────────────────────────────────────────────────────┐
│  Executor JVM: spark.executor.memory = 4GB          │
│  ┌─────────────────────────────────────────────┐    │
│  │ Reserved: 300MB (hardcoded, không đụng vào) │    │
│  ├─────────────────────────────────────────────┤    │
│  │ Usable ≈ 3.7GB                              │    │
│  │                                             │    │
│  │ memory.fraction=0.6 → Unified Pool = 2.2GB  │    │
│  │ ┌──────────────────┬──────────────────────┐ │    │
│  │ │ Storage (30%)    │ Execution (70%)       │ │    │
│  │ │ 0.66GB           │ 1.54GB                │ │    │
│  │ │ cache, broadcast │ shuffle, join, sort   │ │    │
│  │ │ ← mượn nhau được nếu 1 bên rảnh →       │ │    │
│  │ └──────────────────┴──────────────────────┘ │    │
│  │                                             │    │
│  │ User Memory (40%): 1.48GB                   │    │
│  │ → Python UDFs, Spark internal metadata      │    │
│  └─────────────────────────────────────────────┘    │
│                                                     │
│  memoryOverhead = 1GB (off-heap, ngoài JVM)         │
│  → Python worker subprocess (PySpark)               │
│  → JVM native: metaspace, code cache                │
│  → Network buffers                                  │
│                                                     │
│  Tổng OS memory = 4 + 1 = 5GB per executor          │
└─────────────────────────────────────────────────────┘
```

### OOM Troubleshooting
```
Container killed by YARN for exceeding memory:
  → Nguyên nhân 1: memoryOverhead nhỏ → tăng lên 1g → 2g
  → Nguyên nhân 2: shuffle quá lớn   → tăng memory.fraction lên 0.7
  → Nguyên nhân 3: cache nhiều DF lớn → unpersist() sau khi xong
  → Nguyên nhân 4: Python UDF nặng   → chuyển sang Pandas UDF (vectorized)
```

### Job Checklist (bắt buộc mọi Spark job)
```
□ filter partition columns (year/month/day) đầu tiên
□ select() chỉ lấy columns cần, ngay sau khi đọc
□ broadcast() cho mọi dimension table join (mcc, cards nhỏ)
□ AQE enabled (spark.sql.adaptive.enabled = true)
□ sortWithinPartitions(user_id, year, month, day) trước khi write
□ repartition(partition_cols) trước partitionBy write
□ file output size ~128MB
□ không cache DataFrame lớn
□ unpersist() sau khi cache xong việc
□ dynamic partition overwrite mode = dynamic
```

---

## Idempotency Rules

- Ingestion: `_batch_id` unique per run → check trước khi write → skip nếu đã tồn tại
- Spark transformation: `partitionOverwriteMode = dynamic` → overwrite đúng partition, atomic
- dbt: `incremental` strategy + `unique_key = transaction_id` → upsert, không duplicate
- Compaction: read → dedup → overwrite đúng partition → verify → done

---

## Tech Stack

| Component        | Tool                   | Version | Port  |
|------------------|------------------------|---------|-------|
| Source DB        | PostgreSQL             | 14      | 5432  |
| Processing       | Apache Spark (PySpark) | 3.5     | 7077  |
| Storage          | HDFS (Hadoop)          | 3.2     | 9000  |
| Table Catalog    | Apache Hive Metastore  | 3.1     | 9083  |
| Query Engine     | Trino                  | 435     | 8082  |
| Transformation   | dbt-trino              | 1.7     | —     |
| Orchestration    | Apache Airflow         | 2.8     | 8083  |
| Data Quality     | Great Expectations     | 0.18    | —     |
| Visualization    | Apache Superset        | 3.1     | 8088  |
| Containerization | Docker Compose         | —       | —     |
| Config Mgmt      | Hydra (OmegaConf)      | 1.3     | —     |
| Logging          | structlog              | 23.2    | —     |
| Testing          | pytest + chispa        | —       | —     |

---

## Project Structure (actual)

```
DE_Project/
├── .kiro/steering/
│   └── pipeline_architecture.md     ← file này
│
├── data/raw/                         ← raw CSV/JSON files (nguồn gốc)
│
├── docker/
│   ├── docker-compose.yml            ← full stack: source-db+HDFS+Hive+Spark+Trino+Airflow+Superset
│   ├── source-db/
│   │   └── init/
│   │       ├── 01_schema.sql         ← tạo 5 tables (all TEXT columns)
│   │       └── 02_load_data.sql      ← COPY CSV + flatten JSON vào tables
│   ├── hadoop/
│   │   ├── core-site.xml             ← fs.defaultFS = hdfs://namenode:9000
│   │   └── hdfs-site.xml             ← replication=1, blocksize=128MB
│   ├── hive/
│   │   └── hive-site.xml             ← metastore → PostgreSQL backend
│   ├── trino/
│   │   ├── config.properties         ← single-node coordinator
│   │   └── catalog/hive.properties   ← connect Hive Metastore
│   └── spark/
│       ├── spark-defaults.conf       ← AQE, broadcast, dynamic overwrite
│       ├── download_jars.sh          ← download postgresql JDBC jar
│       └── jars/                     ← postgresql-42.7.1.jar (gitignored)
│
├── ingestion/
│   ├── base_ingester.py              ← Abstract base, Template Method pattern
│   ├── jdbc_ingester.py              ← JDBCIngester + 5 concrete ingesters
│   ├── spark_session.py              ← SparkSessionFactory singleton
│   └── schema/
│       ├── transactions_schema.py    ← RAW_CSV_SCHEMA + STAGING_SCHEMA
│       ├── users_schema.py
│       ├── cards_schema.py
│       └── mcc_schema.py
│
├── transformation/
│   ├── staging/
│   │   ├── amount_parser.py          ← AmountParser, AmountFormat enum
│   │   ├── clean_transactions.py     ← cast types, PCI mask, dedup, quality flag
│   │   └── enrich_transactions.py    ← broadcast join mcc/cards/fraud_labels
│   ├── warehouse/
│   │   ├── build_fact_transactions.py
│   │   ├── build_dim_users.py        ← SCD Type 2
│   │   ├── build_dim_cards.py        ← SCD Type 1
│   │   └── build_fraud_features.py   ← window functions
│   └── compaction/
│       └── compactor.py              ← HDFSCompactor, daily + monthly
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml                  ← Trino connection
│   ├── models/
│   │   ├── staging/                  ← stg_transactions, stg_users, stg_cards
│   │   ├── warehouse/                ← fact_transactions, dim_*, agg_*
│   │   └── marts/                    ← fraud_features, user_daily_stats, merchant_risk
│   └── tests/
│
├── airflow/dags/
│   ├── fraud_pipeline_dag.py         ← @daily: ingest → staging → warehouse
│   ├── compaction_pipeline_dag.py    ← @daily @ 03:00 AM
│   └── utils/
│
├── quality/
│   ├── expectations/                 ← Great Expectations suites
│   └── checks/
│
├── config/
│   ├── pipeline.yaml
│   ├── spark.yaml                    ← Spark configs (AQE, broadcast, ...)
│   ├── hdfs.yaml                     ← HDFS paths cho tất cả tables/layers
│   ├── hive.yaml                     ← databases, compression settings
│   └── source_db.yaml                ← JDBC connection + parallel read config
│
├── tests/
│   ├── unit/
│   └── integration/
│
├── docs/
│   └── pipeline_explained.html       ← visual guide, mở bằng browser
│
├── ENGINEERING_LOG.md                ← nhật ký quyết định kỹ thuật
├── Makefile                          ← make up/down/ingest/transform/test
└── requirements.txt
```

---

## Key Design Decisions

| Decision | Choice | Reason |
|---|---|---|
| Source layer | PostgreSQL (source-db) | Giả lập operational DB thực tế, không đọc CSV thẳng |
| JDBC parallel read | Range split trên numeric id | Single-thread JDBC quá chậm với bảng lớn |
| Storage format | Parquet + Snappy | Columnar, predicate pushdown, compress tốt |
| Partition transactions | year/month/day | Time range là use case query chính |
| Partition cards | card_brand/expires_year | Fraud cluster theo card type |
| Query engine | Trino over Hive | MPP, không MapReduce, 10-100x faster |
| Transformation | dbt-trino | SQL lineage, testable, incremental |
| Dedup key | transaction_id | Unique per transaction, idempotent |
| SCD strategy | Type 2 cho users | Track lịch sử thay đổi user info |
| Amount cleaning | 5 cột + quarantine | Không tự sửa ambiguous, không drop record |
| Spark joins | broadcast cho dims | Eliminate shuffle cho bảng nhỏ |
| AQE | Luôn bật | Runtime re-optimization miễn phí |
| Compaction | Daily + monthly | Tránh small file problem trên HDFS NameNode |
| Tiered storage | Hot/Warm/Cold | Giảm cost, data cũ ít query không cần SSD |

---

*Living document — cập nhật mỗi khi có architectural decision mới.*
