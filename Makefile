# Makefile — shortcut commands cho toàn bộ project
# Chạy: make <target>
# Ví dụ: make up, make pipeline

.PHONY: help up down ps logs ingest transform transform-full transform-warehouse \
        compact hive-init hive-repair dbt-run dbt-test pipeline superset-init \
        airflow-init airflow-install-dbt test clean

SPARK_MASTER  := spark://spark-master:7077
SPARK_JAR     := /opt/spark/extra-jars/postgresql-42.7.1.jar
WORK_DIR      := /opt/spark/work-dir
SPARK_SUBMIT  := docker exec spark-master spark-submit --master $(SPARK_MASTER) --jars $(SPARK_JAR)
DBT           := docker exec -u airflow airflow-webserver /home/airflow/.local/bin/dbt
DBT_DIR       := --profiles-dir /home/airflow/dbt --project-dir /home/airflow/dbt --target dev

# Default target
help:
	@echo "Fintech Fraud Detection Pipeline"
	@echo ""
	@echo "Setup (chạy lần đầu theo thứ tự):"
	@echo "  make download-jars  — Download PostgreSQL JDBC driver"
	@echo "  make copy-data      — Copy CSV/JSON vào source-db init"
	@echo "  make up             — Start toàn bộ Docker stack"
	@echo "  make hdfs-init      — Tạo HDFS dirs + Storage Policies"
	@echo "  make hive-init      — Tạo Hive external tables"
	@echo ""
	@echo "Pipeline end-to-end:"
	@echo "  make pipeline       — Full backfill: ingest → transform → warehouse → dbt"
	@echo "  bash scripts/run_e2e.sh  — Same as pipeline (with smoke test)"
	@echo ""
	@echo "Pipeline steps:"
	@echo "  make ingest             — JDBC: source DB → HDFS raw"
	@echo "  make transform-full     — Full backfill: raw → staging (static dataset)"
	@echo "  make transform-warehouse — Spark: fraud features"
	@echo "  make hive-repair        — MSCK REPAIR staging/warehouse partitions"
	@echo "  make dbt-run            — dbt: staging → warehouse → marts"
	@echo "  make dbt-test           — dbt data tests"
	@echo "  make superset-init      — Register Trino connection in Superset"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make up / down / ps / logs / hdfs-ls"

# ---- Infrastructure ----

download-jars:
	@echo "Downloading JDBC drivers..."
	bash docker/spark/download_jars.sh

copy-data:
	@echo "Copying raw data files into source-db init folder..."
	mkdir -p docker/source-db/init/data
	cp data/raw/transactions_data.csv docker/source-db/init/data/
	cp data/raw/users_data.csv docker/source-db/init/data/
	cp data/raw/cards_data.csv docker/source-db/init/data/
	cp data/raw/mcc_codes.json docker/source-db/init/data/
	cp data/raw/train_fraud_labels.json docker/source-db/init/data/
	@echo "Done. Files in docker/source-db/init/data/:"
	ls -lh docker/source-db/init/data/

prepare-build:
	@echo "Copying hadoop configs into service build contexts..."
	mkdir -p docker/spark/hadoop-configs
	cp docker/hadoop/core-site.xml docker/spark/hadoop-configs/
	cp docker/hadoop/hdfs-site.xml docker/spark/hadoop-configs/
	@echo "Build contexts ready."

up:
	@echo "Starting DE stack..."
	$(MAKE) prepare-build
	docker compose -f docker/docker-compose.yml up -d --build
	@echo ""
	@echo "Services:"
	@echo "  Source DB:      postgresql://localhost:5432/fintech_source  (fintech/fintech)"
	@echo "  HDFS Web UI:    http://localhost:9870"
	@echo "  Spark Web UI:   http://localhost:8081"
	@echo "  Trino UI:       http://localhost:8082"
	@echo "  Airflow UI:     http://localhost:8083  (admin/admin)"
	@echo "  Superset UI:    http://localhost:8088  (admin/admin)"

source-db-check:
	@echo "Checking source DB row counts..."
	docker exec source-db psql -U fintech -d fintech_source -c \
		"SELECT tablename, n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC;"

down:
	docker compose -f docker/docker-compose.yml down

down-clean:
	@echo "WARNING: This will delete all volumes (HDFS data, Metastore, Airflow state)"
	docker compose -f docker/docker-compose.yml down -v

ps:
	docker compose -f docker/docker-compose.yml ps

logs:
	docker compose -f docker/docker-compose.yml logs -f

logs-%:
	docker compose -f docker/docker-compose.yml logs -f $*

# ---- HDFS / Hive ----

hdfs-init:
	@echo "Setting up HDFS directories and storage policies..."
	bash docker/hadoop/setup_storage_policies.sh

hdfs-policies:
	docker exec namenode hdfs storagepolicies -getStoragePolicy -path /data/lake/raw
	docker exec namenode hdfs storagepolicies -getStoragePolicy -path /data/lake/staging
	docker exec namenode hdfs storagepolicies -getStoragePolicy -path /data/lake/warm
	docker exec namenode hdfs storagepolicies -getStoragePolicy -path /data/lake/cold

hdfs-ls:
	docker exec namenode hdfs dfs -ls -R /data/lake/

hive-init:
	@echo "Creating Hive databases and external tables..."
	cat docker/hive/init_hive_schemas.sql | docker exec -i hive-server \
		beeline -u jdbc:hive2://localhost:10000 --silent=true
	@echo "Hive schemas created."

hive-repair:
	@echo "Syncing Hive partitions..."
	docker exec hive-server beeline -u jdbc:hive2://localhost:10000 --silent=true -e "\
		MSCK REPAIR TABLE staging.transactions; \
		MSCK REPAIR TABLE staging.users; \
		MSCK REPAIR TABLE staging.cards; \
		MSCK REPAIR TABLE warehouse.feat_fraud_features; \
	"

# ---- Pipeline ----

ingest:
	@echo "Running JDBC ingestion (source DB → HDFS raw)..."
	$(SPARK_SUBMIT) $(WORK_DIR)/ingestion/jdbc_ingester.py

transform-full:
	@echo "Full backfill: raw → staging..."
	@echo "Step 1/4: clean_transactions_full..."
	$(SPARK_SUBMIT) $(WORK_DIR)/transformation/staging/clean_transactions_full.py
	@echo "Step 2/4: clean_users..."
	$(SPARK_SUBMIT) $(WORK_DIR)/transformation/staging/clean_users.py
	@echo "Step 3/4: clean_cards..."
	$(SPARK_SUBMIT) $(WORK_DIR)/transformation/staging/clean_cards.py
	@echo "Step 4/4: enrich_transactions_full..."
	$(SPARK_SUBMIT) $(WORK_DIR)/transformation/staging/enrich_transactions_full.py
	@echo "Transform complete."

transform-warehouse:
	@echo "Building warehouse features (Spark)..."
	$(SPARK_SUBMIT) $(WORK_DIR)/transformation/warehouse/build_fraud_features.py
	@echo "Warehouse build complete."

compact:
	@echo "Running compaction..."
	$(SPARK_SUBMIT) $(WORK_DIR)/transformation/compaction/compactor.py

dbt-run:
	@echo "Running dbt models..."
	$(DBT) run $(DBT_DIR) --vars '{"execution_date": "2019-12-31"}'

dbt-test:
	@echo "Running dbt tests..."
	$(DBT) test $(DBT_DIR)

pipeline: ingest transform-full transform-warehouse hive-repair dbt-run dbt-test
	@echo "Full pipeline complete."

superset-init:
	bash scripts/init_superset_trino.sh

airflow-init:
	@echo "Registering Airflow Spark connection..."
	docker exec airflow-webserver airflow connections delete spark_default 2>/dev/null || true
	docker exec airflow-webserver airflow connections add spark_default \
		--conn-type spark --conn-host spark-master --conn-port 7077
	@echo "Done."

airflow-install-dbt:
	bash scripts/install_airflow_deps.sh

# ---- Development ----

test:
	SPARK_LOCAL_MODE=true pytest tests/ -v

lint:
	flake8 ingestion/ transformation/ airflow/ --max-line-length=120

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null; true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null; true
