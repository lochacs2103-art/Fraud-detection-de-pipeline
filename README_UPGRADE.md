# Batch Fraud Intelligence Upgrade Pack

This overlay upgrades the existing repository without Kafka or streaming. It
keeps the current PostgreSQL → Spark → HDFS → Hive/Trino → dbt → Superset stack.

## What is implemented

- Point-in-time fraud features that exclude the current transaction.
- A narrow transaction index for selective historical updates.
- Detection and history of late-arriving fraud-label changes.
- Idempotent historical restatement with an applied-change ledger.
- Before/after reporting snapshots (`as previously reported` vs `restated`).
- Source-to-staging reconciliation with a hard accounting invariant.
- An explainable dbt investigation queue with risk score and reason codes.
- A new Airflow DAG that ties the batch workflow together.
- An integration test proving that feature leakage is prevented.

## Apply to the repository

From the root of this upgrade pack:

```bash
cp -R airflow transformation quality dbt scripts tests docs /path/to/Fraud-detection-de-pipeline/
```

The file `transformation/warehouse/build_fraud_features.py` intentionally
replaces the existing implementation. Back it up first when applying manually.

## Initialize Hive audit tables

After the Docker stack is running, create audit DDL via Spark SQL
(hive-server/beeline is optional on this stack — Thrift :10000 often fails to bind on WSL):

```bash
docker exec -e PROJECT_ROOT=/opt/spark/work-dir spark-master \
  /opt/spark/bin/spark-sql --master local[1] \
  -f /opt/spark/work-dir/scripts/sql/create_audit_tables.sql
```

Adjust the mounted project path if your Spark container uses another location.
Then run `MSCK REPAIR TABLE` once for audit tables that already contain data
(`make hive-repair` or `scripts/msck_repair.py`).

## Activate the new DAG

Pause `fraud_data_pipeline` during testing and enable:

```text
fraud_auditable_batch_pipeline
```

Both DAGs should not process the same logical date concurrently because both
write the same dynamic transaction partitions.

## One-time historical bootstrap

Before enabling the new DAG, build the transaction lookup index for existing
staging history:

```bash
spark-submit transformation/audit/build_transaction_index.py --full
```

Then run the DAG once. Its first fraud-label run creates a baseline history and
does **not** treat all existing labels as late-arriving changes.

## Recommended validation order

```bash
python -m compileall transformation quality airflow
pytest tests/integration/test_point_in_time_features.py -q

dbt --project-dir dbt --profiles-dir dbt compile
dbt --project-dir dbt --profiles-dir dbt test
```

Then execute one known date through Airflow and verify:

```sql
SELECT *
FROM hive.warehouse.batch_reconciliation_results
ORDER BY event_date DESC;
```

The most important acceptance criterion is:

```text
unexplained_difference = 0
```

## Suggested CV bullet

> Built an auditable batch fraud intelligence platform with point-in-time feature engineering, idempotent late-label restatements, source-to-warehouse reconciliation, historical report versioning and an explainable investigation queue.
