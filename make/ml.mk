# ==============================================================================
# make/ml.mk: Feast offline và Redis online. Không train model, không KServe.
# ==============================================================================

.PHONY: feast-offline-it feast-redis-up feast-online-it feast-airflow-it

feast-redis-up: ## Redis local :16379, db 1, không đụng serving
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) -f infra/compose/docker-compose.feast.yml up -d --no-deps --wait redis

feast-offline-it: ## Feast 0.67: apply, point-in-time, đối chiếu snapshot file://
	@echo -e "$(BOLD)$(BLUE)Chạy kiểm tra Feast offline...$(RESET)"
	FEAST_USAGE=False PYTHONPATH=. $(UV) run --no-project --python 3.12 --with 'feast[duckdb]==0.67.0' --with pyarrow python -m unittest tests.ml.test_feast_offline
	SPARK_LOCAL_IP=127.0.0.1 FEAST_USAGE=False PYTHONUNBUFFERED=1 PYTHONPATH=. $(UV) run --no-project --python 3.12 --with 'feast[duckdb]==0.67.0' --with pyspark==3.5.5 --with pyarrow --with setuptools python -m ml.offline.it_feast_snapshot

feast-online-it: feast-redis-up ## Materialize snapshot sang Redis và get_online_features
	@echo -e "$(BOLD)$(BLUE)Chạy kiểm tra Feast online...$(RESET)"
	FEAST_USAGE=False PYTHONPATH=. $(UV) run --no-project --python 3.12 --with 'feast[duckdb,redis]==0.67.0' --with pyarrow python -m unittest tests.ml.test_publication tests.ml.test_materialize
	SPARK_LOCAL_IP=127.0.0.1 FEAST_USAGE=False PYTHONUNBUFFERED=1 PYTHONPATH=. $(UV) run --no-project --python 3.12 --with 'feast[duckdb,redis]==0.67.0' --with pyspark==3.5.5 --with pyarrow --with setuptools python -m ml.online.it_feast_online

feast-airflow-it: feast-redis-up ## DP1–DP4 trên it_af_dp_b2, materialize Redis, không ghi production
	PYTHONPATH=. $(UV) run --no-project --python 3.12 --with pyspark==3.5.5 --with boto3 --with pandas --with pyarrow --with setuptools python -m orchestration.it_feast_airflow
