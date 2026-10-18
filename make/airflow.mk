# ==============================================================================
# make/airflow.mk: Airflow LocalExecutor điều phối job Spark có sẵn
# ==============================================================================

AIRFLOW_COMPOSE = -f $(COMPOSE_FILE) -f infra/compose/docker-compose.airflow.yml

##@ 5. Điều phối Airflow

.PHONY: airflow-up airflow-down airflow-ps airflow-it airflow-it-real

airflow-up: ## Bật MinIO, Iceberg REST, Spark và Airflow (UI :18082, user airflow)
	$(DOCKER_COMPOSE) $(AIRFLOW_COMPOSE) up -d --build --wait minio iceberg-rest spark-master spark-worker airflow-webserver airflow-scheduler

airflow-down: ## Dừng Airflow và Postgres metadata, giữ MinIO
	$(DOCKER_COMPOSE) $(AIRFLOW_COMPOSE) stop airflow-scheduler airflow-webserver airflow-postgres

airflow-ps: ## Trạng thái cụm kèm Airflow
	$(DOCKER_COMPOSE) $(AIRFLOW_COMPOSE) ps

airflow-it: ## Test DAG và fixture nhỏ trên Spark, Iceberg REST, MinIO
	PYTHONPATH=. python3 -m orchestration.test_batch_identity
	PYTHONPATH=. $(UV) run --no-project --python 3.12 --with pyspark==3.5.5 --with boto3 python -m orchestration.it_phase2

airflow-it-real: ## Nguồn Parquet thật, namespace it_af_real, không ghi production
	PYTHONPATH=. $(UV) run --no-project --python 3.12 --with pyspark==3.5.5 python tests/unit/test_timestamp_contract.py
	PYTHONPATH=. $(UV) run --no-project --python 3.12 --with pyspark==3.5.5 --with boto3 --with pyarrow python -m orchestration.it_real_source
