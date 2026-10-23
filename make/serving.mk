# ==============================================================================
# make/serving.mk: ClickHouse phục vụ Gold đã publish
# ==============================================================================

SERVING_COMPOSE = -f $(COMPOSE_FILE) -f infra/compose/docker-compose.clickhouse.yml

##@ 6. Serving ClickHouse

.PHONY: serving-up serving-down serving-api serving-dashboard serving-dashboard-up serving-dashboard-down serving-it serving-it-api serving-it-audit serving-trending-approve serving-trending-sync serving-it-trending serving-it-ops serving-it-reliability serving-it-dashboard serving-it-unified

serving-up: ## Bật ClickHouse :18123 và tạo cinestream_serving
	$(DOCKER_COMPOSE) $(SERVING_COMPOSE) up -d --no-deps --wait clickhouse
	sed 's/__DATABASE__/cinestream_serving/g' serving/clickhouse/ddl.sql \
		| $(DOCKER_COMPOSE) $(SERVING_COMPOSE) exec -T clickhouse \
			clickhouse-client --user default --password cinestream --multiquery

serving-down: ## Dừng ClickHouse, giữ MinIO và Iceberg
	$(DOCKER_COMPOSE) $(SERVING_COMPOSE) stop clickhouse

serving-api: ## API :18083, gồm /governance chỉ đọc. Batch là CINESTREAM_CLICKHOUSE_DATABASE
	PYTHONPATH=. $(UV) run --no-project --python 3.12 --with fastapi --with 'uvicorn[standard]' --with boto3 uvicorn serving.api.app:app --host 127.0.0.1 --port 18083

serving-dashboard: ## Dashboard :18084, proxy tới CINESTREAM_API_URL hoặc :18083
	PYTHONPATH=. $(UV) run --no-project --python 3.12 python -m serving.dashboard.server

serving-dashboard-up: ## Nginx dashboard :18085, FastAPI vẫn ở host :18083
	docker compose -f infra/compose/docker-compose.dashboard.yml up -d

serving-dashboard-down: ## Tắt nginx dashboard, không đụng ClickHouse
	docker compose -f infra/compose/docker-compose.dashboard.yml down

serving-it: serving-up ## Test sync snapshot trên it_ch_gold và database it_ch
	SPARK_LOCAL_IP=127.0.0.1 PYTHONUNBUFFERED=1 PYTHONPATH=. $(UV) run --no-project --python 3.12 --with pyspark==3.5.5 --with boto3 python -m serving.clickhouse.it_phase3a

serving-it-api: serving-up ## Test API và sync trên it_api_gold, không ghi production
	SPARK_LOCAL_IP=127.0.0.1 PYTHONUNBUFFERED=1 PYTHONPATH=. $(UV) run --no-project --python 3.12 --with pyspark==3.5.5 --with boto3 --with fastapi --with 'uvicorn[standard]' python -m serving.clickhouse.it_phase3b

serving-it-audit: serving-up ## Audit phục hồi Phase 3B trên it_audit_gold
	SPARK_LOCAL_IP=127.0.0.1 PYTHONUNBUFFERED=1 PYTHONPATH=. $(UV) run --no-project --python 3.12 --with pyspark==3.5.5 --with boto3 --with fastapi --with 'uvicorn[standard]' python -m serving.clickhouse.it_phase3b_audit

serving-trending-approve: ## Chặn poll production nếu chưa có CINESTREAM_ALLOW_STREAMING=true
	@test "$(CINESTREAM_ALLOW_STREAMING)" = "true" || { echo "CINESTREAM_ALLOW_STREAMING chưa bật. Không poll stg_playback_windows vào cinestream_serving."; exit 1; }

serving-trending-sync: serving-trending-approve serving-up ## Poll production mỗi CINESTREAM_TRENDING_INTERVAL_SECONDS giây. Ctrl-C để dừng
	SPARK_LOCAL_IP=127.0.0.1 PYTHONUNBUFFERED=1 PYTHONPATH=. CINESTREAM_ALLOW_STREAMING=true $(UV) run --no-project --python 3.12 --with pyspark==3.5.5 --with boto3 python -m serving.clickhouse.sync_windows --interval-seconds $${CINESTREAM_TRENDING_INTERVAL_SECONDS:-30}

serving-it-trending: serving-up stream-up ## Test trending trên topic it.p2.trending và database it_trend
	SPARK_LOCAL_IP=127.0.0.1 PYTHONUNBUFFERED=1 PYTHONPATH=. $(UV) run --no-project --python 3.12 --with pyspark==3.5.5 --with boto3 --with kafka-python --with fastapi --with 'uvicorn[standard]' python -m serving.clickhouse.it_phase3c

serving-it-ops: serving-up stream-up ## Sẵn sàng vận hành trending trên it.p2.ops, không bật production
	SPARK_LOCAL_IP=127.0.0.1 PYTHONUNBUFFERED=1 PYTHONPATH=. $(UV) run --no-project --python 3.12 --with pyspark==3.5.5 --with boto3 --with kafka-python --with fastapi --with 'uvicorn[standard]' python -m serving.clickhouse.it_phase3d

serving-it-reliability: serving-up stream-up ## Checkpoint, late event sau restore, poll 30 giây trên it.p2.e2e
	SPARK_LOCAL_IP=127.0.0.1 PYTHONUNBUFFERED=1 PYTHONPATH=. $(UV) run --no-project --python 3.12 --with pyspark==3.5.5 --with boto3 --with kafka-python --with fastapi --with 'uvicorn[standard]' python -m serving.clickhouse.it_phase3e

serving-it-dashboard: serving-up ## Đối chiếu dashboard với FastAPI, không bật streaming
	PYTHONPATH=. $(UV) run --no-project --python 3.12 --with fastapi --with 'uvicorn[standard]' python -m serving.dashboard.it_phase4

serving-it-unified: serving-up ## Batch it_it_af_real và trending it_e2e trên một API
	PYTHONPATH=. $(UV) run --no-project --python 3.12 --with fastapi --with 'uvicorn[standard]' python -m serving.dashboard.it_phase4b
