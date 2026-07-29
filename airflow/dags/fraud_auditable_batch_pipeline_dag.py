"""
fraud_auditable_batch_pipeline_dag.py — Auditable batch fraud intelligence.

Do NOT run concurrently with fraud_data_pipeline for the same logical date
(both write staging transaction partitions).

Flow (knowledge_date = ds):
  build_transaction_index
    → detect_fraud_label_changes
    → restate_affected_partitions
    → reconcile_batch
    → build_fraud_features (PIT)
    → dbt marts (investigation queue / reports)
    → dbt test (reconciliation invariant)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from dags.utils.spark_utils import make_spark_submit, PROJECT_ROOT

default_args = {
    "owner": "de_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# Prefer /tmp/dbt when host bind-mount of /home/airflow/dbt is stale
DBT_CMD = (
    "/home/airflow/.local/bin/dbt "
    "--profiles-dir /tmp/dbt "
    "--project-dir /tmp/dbt "
    "--target dev "
    '--vars \'{"execution_date": "{{ ds }}"}\''
)

AUDIT_MODELS = (
    "investigation_queue restatement_report "
    "fraud_label_impact batch_reconciliation"
)

with DAG(
    dag_id="fraud_auditable_batch_pipeline",
    description="Auditable batch: late labels, restatement, recon, PIT features, investigation queue",
    schedule_interval="0 3 * * *",
    start_date=datetime(2018, 6, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fraud", "audit", "batch"],
) as dag:

    build_index = make_spark_submit(
        task_id="build_transaction_index",
        application=f"{PROJECT_ROOT}/transformation/audit/build_transaction_index.py",
        application_args=["{{ ds }}"],
        extra_conf={"spark.app.name": "txn_index_{{ ds }}"},
    )

    detect_labels = make_spark_submit(
        task_id="detect_fraud_label_changes",
        application=f"{PROJECT_ROOT}/transformation/audit/detect_fraud_label_changes.py",
        application_args=["{{ ds }}"],
        extra_conf={"spark.app.name": "detect_labels_{{ ds }}"},
    )

    restate = make_spark_submit(
        task_id="restate_affected_partitions",
        application=f"{PROJECT_ROOT}/transformation/audit/restate_affected_partitions.py",
        application_args=["{{ ds }}"],
        extra_conf={"spark.app.name": "restate_{{ ds }}"},
    )

    reconcile = make_spark_submit(
        task_id="reconcile_batch",
        application=f"{PROJECT_ROOT}/transformation/audit/reconcile_batch.py",
        application_args=["{{ ds }}"],
        extra_conf={"spark.app.name": "reconcile_{{ ds }}"},
    )

    build_features = make_spark_submit(
        task_id="build_fraud_features_pit",
        application=f"{PROJECT_ROOT}/transformation/warehouse/build_fraud_features.py",
        application_args=["{{ ds }}"],
        extra_conf={"spark.app.name": "fraud_features_pit_{{ ds }}"},
    )

    dbt_sync = BashOperator(
        task_id="sync_dbt_to_tmp",
        bash_command=(
            "cp -a /opt/project/dbt/. /tmp/dbt/ 2>/dev/null || "
            "cp -a /home/airflow/dbt/. /tmp/dbt/; "
            "echo synced"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run_audit_marts",
        bash_command=f"{DBT_CMD} run --select {AUDIT_MODELS} --log-path /tmp/dbt/logs",
    )

    dbt_test = BashOperator(
        task_id="dbt_test_reconciliation",
        bash_command=(
            f"{DBT_CMD} test --select assert_reconciliation_invariant "
            f"--log-path /tmp/dbt/logs"
        ),
    )

    notify = BashOperator(
        task_id="notify_success",
        bash_command="echo 'Auditable batch SUCCESS for knowledge_date={{ ds }}'",
    )

    (
        build_index
        >> detect_labels
        >> restate
        >> reconcile
        >> build_features
        >> dbt_sync
        >> dbt_run
        >> dbt_test
        >> notify
    )
