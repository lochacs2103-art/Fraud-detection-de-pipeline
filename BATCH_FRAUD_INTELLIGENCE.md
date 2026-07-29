# Auditable Batch Fraud Intelligence

## Business scenario

Transactions arrive every day, but a fraud result can be confirmed several days
later after investigation or chargeback. The platform must update historical
analytics without losing the report that the business originally saw.

The design separates:

- **Event time:** when the transaction occurred.
- **Knowledge time:** when the fraud result became known.

## Daily flow

1. Ingest source snapshots through Spark JDBC.
2. Clean and quarantine invalid transaction records.
3. Enrich the event-date partition.
4. Build a narrow transaction index.
5. Compare the latest fraud-label snapshot with the fraud-label history.
6. Create an impact manifest for changed labels.
7. Restate only affected transaction partitions.
8. Store before/after report snapshots.
9. Build point-in-time-correct fraud features.
10. Reconcile every input row as accepted, quarantined or duplicate.
11. Build the investigation queue and restatement report with dbt.

## Core invariants

```text
raw_count - duplicate_count
= accepted_count + quarantined_count
```

```text
A transaction must not contribute to its own historical fraud features.
```

```text
Every historical report change must have a change_id, event_date,
knowledge_date, old value and new value.
```

## Demo that should appear in the README

1. Run the pipeline for Day 1 with a transaction whose fraud label is unknown.
2. Show the original daily fraud report.
3. On Day 3, change that transaction's source label to `Yes`.
4. Run the Day 3 batch.
5. Show:
   - the detected impact manifest;
   - the affected Day 1 partition only;
   - before vs restated fraud count and amount;
   - reconciliation with `unexplained_difference = 0`;
   - the transaction appearing in the investigation queue with reason codes.

## Ownership boundary

- **Spark:** ingestion, cleansing, quarantine, point-in-time features,
  reconciliation and restatement mechanics.
- **dbt:** business-facing facts, marts, investigation queue and reporting views.
- **Airflow:** deterministic orchestration by logical date.
- **HDFS/Hive:** Parquet storage, audit history and queryable metadata.
