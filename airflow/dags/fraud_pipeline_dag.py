"""
fraud_pipeline_dag.py — Main daily pipeline DAG.

Schedule: @daily lúc 02:00 AM
Catchup: True → Airflow tự backfill từng ngày nếu bị miss

Lưu ý: Backfill lần đầu (static dataset) dùng Makefile / scripts/run_e2e.sh.
DAG này dùng cho daily incremental sau khi data đã có trên HDFS.

Flow:
  ingest_all → validate_raw → staging → hive_repair
            → build_features → dbt → dbt_test → notify
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup

from dags.utils.spark_utils import make_spark_submit, PROJECT_ROOT

default_args = {
    "owner":            "de_team",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
}

DBT_CMD = (
    "/home/airflow/.local/bin/dbt "
    "--profiles-dir /home/airflow/dbt "
    "--project-dir /home/airflow/dbt "
    "--target dev "
    '--vars \'{"execution_date": "{{ ds }}"}\''
)

with DAG(
    dag_id="fraud_data_pipeline",
    description="Daily fraud detection data pipeline: ingest → staging → warehouse",
    schedule_interval="0 2 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=True,
    max_active_runs=4,
    default_args=default_args,
    tags=["fraud", "daily", "core"],
) as dag:

    ingest_all = make_spark_submit(
        task_id="ingest_all",
        application=f"{PROJECT_ROOT}/ingestion/jdbc_ingester.py",
        extra_conf={"spark.app.name": "ingest_all_{{ ds }}"},
    )

    validate_raw = BashOperator(
        task_id="validate_raw",
        bash_command=(
            f"python {PROJECT_ROOT}/quality/checks/run_checks.py "
            "--layer raw --date {{ ds }}"
        ),
    )

    with TaskGroup("staging", tooltip="Clean + Enrich → HDFS staging") as staging_group:

        clean_txn = make_spark_submit(
            task_id="clean_transactions",
            application=f"{PROJECT_ROOT}/transformation/staging/clean_transactions.py",
            application_args=["{{ ds }}"],
            extra_conf={"spark.app.name": "clean_transactions_{{ ds }}"},
        )

        enrich_txn = make_spark_submit(
            task_id="enrich_transactions",
            application=f"{PROJECT_ROOT}/transformation/staging/enrich_transactions.py",
            application_args=["{{ ds }}"],
        )

        clean_txn >> enrich_txn

    hive_repair = make_spark_submit(
        task_id="hive_msck_repair",
        application=f"{PROJECT_ROOT}/scripts/msck_repair.py",
        extra_conf={"spark.app.name": "msck_repair_{{ ds }}"},
    )

    build_features = make_spark_submit(
        task_id="build_fraud_features",
        application=f"{PROJECT_ROOT}/transformation/warehouse/build_fraud_features.py",
        application_args=["{{ ds }}"],
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT_CMD} run --log-path /home/airflow/dbt/logs",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_CMD} test --log-path /home/airflow/dbt/logs",
    )

    def _check_quarantine(**context):
        quarantine_count = 0
        if quarantine_count > 0:
            return "flag_quarantine"
        return "notify_success"

    branch = BranchPythonOperator(
        task_id="check_quarantine",
        python_callable=_check_quarantine,
    )

    flag_quarantine = BashOperator(
        task_id="flag_quarantine",
        bash_command="echo 'QUARANTINE RECORDS FOUND on {{ ds }} — review needed'",
    )

    notify_success = BashOperator(
        task_id="notify_success",
        bash_command="echo 'Pipeline SUCCESS for {{ ds }}'",
    )

    ingest_all >> validate_raw >> staging_group >> hive_repair
    hive_repair >> build_features >> dbt_run >> dbt_test >> branch
    branch >> [notify_success, flag_quarantine]
