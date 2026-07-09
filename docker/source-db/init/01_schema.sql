-- ============================================================
-- Source Database Schema
-- Giả lập operational database của Fintech
-- Data từ CSV sẽ được COPY vào các tables này
--
-- Tại sao tất cả columns là TEXT?
-- Vì CSV là nguồn thô — không biết chắc format, để PostgreSQL
-- lưu nguyên, Spark sẽ cast về đúng type trong staging layer.
-- Nếu cast ngay ở đây → mất audit trail, khó debug khi có lỗi.
-- ============================================================

-- Drop nếu đã tồn tại (idempotent)
DROP TABLE IF EXISTS raw_transactions CASCADE;
DROP TABLE IF EXISTS raw_users CASCADE;
DROP TABLE IF EXISTS raw_cards CASCADE;
DROP TABLE IF EXISTS raw_mcc_codes CASCADE;
DROP TABLE IF EXISTS raw_fraud_labels CASCADE;

-- ---- Transactions ----
CREATE TABLE raw_transactions (
    id                TEXT,
    date              TEXT,
    client_id         TEXT,
    card_id           TEXT,
    amount            TEXT,    -- raw string, có thể "$1,234.56"
    use_chip          TEXT,
    merchant_id       TEXT,
    merchant_city     TEXT,
    merchant_state    TEXT,
    zip               TEXT,
    mcc               TEXT,
    errors            TEXT,

    -- Metadata columns thêm khi load vào source DB
    -- Giúp trace: file nào, load lúc nào
    _loaded_at        TIMESTAMP DEFAULT NOW(),
    _source_file      TEXT DEFAULT 'transactions_data.csv'
);

-- Index trên client_id và card_id để Spark JDBC parallel read hiệu quả
-- Spark chia query thành nhiều partitions dựa trên một numeric column
-- client_id là TEXT → cần cast hoặc dùng ctid cho parallel read
CREATE INDEX idx_transactions_client_id ON raw_transactions(client_id);
CREATE INDEX idx_transactions_card_id   ON raw_transactions(card_id);
CREATE INDEX idx_transactions_date      ON raw_transactions(date);

-- ---- Users ----
CREATE TABLE raw_users (
    id                  TEXT,
    current_age         TEXT,
    retirement_age      TEXT,
    birth_year          TEXT,
    birth_month         TEXT,
    gender              TEXT,
    address             TEXT,
    latitude            TEXT,
    longitude           TEXT,
    per_capita_income   TEXT,
    yearly_income       TEXT,
    total_debt          TEXT,
    credit_score        TEXT,
    num_credit_cards    TEXT,

    _loaded_at          TIMESTAMP DEFAULT NOW(),
    _source_file        TEXT DEFAULT 'users_data.csv'
);

CREATE INDEX idx_users_id ON raw_users(id);

-- ---- Cards ----
CREATE TABLE raw_cards (
    id                      TEXT,
    client_id               TEXT,
    card_brand              TEXT,
    card_type               TEXT,
    card_number             TEXT,   -- sensitive — sẽ được mask trong staging
    expires                 TEXT,
    cvv                     TEXT,   -- sensitive — sẽ bị drop hoàn toàn trong staging
    has_chip                TEXT,
    num_cards_issued        TEXT,
    credit_limit            TEXT,
    acct_open_date          TEXT,
    year_pin_last_changed   TEXT,
    card_on_dark_web        TEXT,

    _loaded_at              TIMESTAMP DEFAULT NOW(),
    _source_file            TEXT DEFAULT 'cards_data.csv'
);

CREATE INDEX idx_cards_id        ON raw_cards(id);
CREATE INDEX idx_cards_client_id ON raw_cards(client_id);

-- ---- MCC Codes ----
-- JSON được flatten thành table: mcc_code → description
CREATE TABLE raw_mcc_codes (
    mcc_code     TEXT,
    description  TEXT,

    _loaded_at   TIMESTAMP DEFAULT NOW(),
    _source_file TEXT DEFAULT 'mcc_codes.json'
);

CREATE UNIQUE INDEX idx_mcc_codes_code ON raw_mcc_codes(mcc_code);

-- ---- Fraud Labels ----
-- JSON: {transaction_id: is_fraud (0/1)}
CREATE TABLE raw_fraud_labels (
    transaction_id  TEXT,
    is_fraud        TEXT,   -- "0" hoặc "1", cast sang BOOLEAN trong staging

    _loaded_at      TIMESTAMP DEFAULT NOW(),
    _source_file    TEXT DEFAULT 'train_fraud_labels.json'
);

CREATE UNIQUE INDEX idx_fraud_labels_txn ON raw_fraud_labels(transaction_id);
