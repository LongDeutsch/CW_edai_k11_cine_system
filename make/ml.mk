# ==============================================================================
# make/ml.mk: Feast offline, Redis online, ML Training, KServe v2 & RAG Qdrant
# ==============================================================================

.PHONY: feast-offline-it feast-redis-up feast-online-it feast-airflow-it feast-streaming-it ml-train-sets ml-train ml-serve rag-index ml-test

ml-train-sets: ## Trích xuất training sets (train_churn.parquet, train_rec.parquet)
	@echo -e "$(BOLD)$(BLUE)Trích xuất Point-in-Time Join cho ML training...$(RESET)"
	PYTHONPATH=. $(UV) run --with pandas python -m ml.offline.build_training_sets --output-dir data

ml-train: ## Huấn luyện mô hình cơ sở Churn & Recommendation và upload MinIO
	@echo -e "$(BOLD)$(BLUE)Huấn luyện mô hình ML và lưu artifact...$(RESET)"
	PYTHONPATH=. $(UV) run --with pandas --with boto3 python -m ml.training.train_models --data-dir data --artifact-dir artifacts/models

ml-serve: ## Khởi chạy ML Serving KServe v2 protocol trên cổng :18086
	@echo -e "$(BOLD)$(GREEN)Khởi chạy KServe v2 ML Serving...$(RESET)"
	PYTHONPATH=. $(PYTHON) -m serving.ml_serving.app

rag-index: ## Lập chỉ mục HNSW embeddings cho phim lên Qdrant
	@echo -e "$(BOLD)$(BLUE)Xây dựng Movie Embeddings và nạp Qdrant...$(RESET)"
	PYTHONPATH=. $(PYTHON) -m transformation.rag.build_movie_embeddings

ml-test: ## Kiểm thử toàn diện RAG, ML Training & KServe v2 Serving
	@echo -e "$(BOLD)$(CYAN)Chạy bộ kiểm thử ML & RAG...$(RESET)"
	PYTHONPATH=. $(UV) run --with pandas python -m unittest tests.ml.test_rag_embeddings tests.ml.test_training_pipeline tests.ml.test_kserve_serving -v

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

feast-streaming-it: feast-redis-up ## Cửa sổ Flink cô lập → Feast offline + Redis db=2
	FEAST_USAGE=False PYTHONPATH=. $(UV) run --no-project --python 3.12 --with 'feast[duckdb,redis]==0.67.0' --with pyarrow --with pandas python -m unittest tests.ml.test_streaming_sync
	SPARK_LOCAL_IP=127.0.0.1 FEAST_USAGE=False PYTHONUNBUFFERED=1 PYTHONPATH=. $(UV) run --no-project --python 3.12 --with 'feast[duckdb,redis]==0.67.0' --with pyspark==3.5.5 --with pyarrow --with pandas --with setuptools --with boto3 --with kafka-python python -m ml.online.it_streaming_features
