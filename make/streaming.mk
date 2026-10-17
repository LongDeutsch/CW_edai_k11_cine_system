# ==============================================================================
# make/streaming.mk: Flink heartbeat windows, DLQ và Iceberg sink
# ==============================================================================

STREAM_COMPOSE := $(DOCKER_COMPOSE) -f infra/compose/docker-compose.streaming.yml
STREAM_PY := PYTHONPATH=. $(PYTHON)

##@ 6. Streaming Flink (Layer 4)

.PHONY: stream-up stream-down stream-job-run stream-verify stream-test

stream-up: ## Bật JobManager và TaskManager trên mạng cinestream_default
	$(STREAM_COMPOSE) up -d --build

stream-down: ## Tắt cụm Flink (không đụng MinIO/Kafka)
	$(STREAM_COMPOSE) down

stream-job-run: stream-up ## Submit job cửa sổ heartbeat lên JobManager
	$(STREAM_COMPOSE) exec -T jobmanager flink run -d \
		-py /opt/cinestream/streaming/flink/jobs/heartbeat_window_job.py \
		--engine flink \
		--bootstrap kafka:29092 \
		--iceberg-rest http://iceberg-rest:8181 \
		--s3-endpoint http://minio:9000

stream-verify: ## Chứng minh late data vào DLQ; đọc topic và bucket nếu cụm đang chạy
	@echo -e "$(BOLD)$(BLUE)Watermark + cửa sổ trên chuỗi timestamp lộn xộn$(RESET)"
	$(STREAM_PY) -m streaming.flink.jobs.heartbeat_window_job --engine local --scenario proof
	@echo -e "$(BOLD)$(BLUE)Topic DLQ và metadata Silver (bỏ qua nếu dịch vụ tắt)$(RESET)"
	@PYTHONPATH=. $(UV) run --extra kafka --extra minio python -m streaming.flink.verify || echo -e "$(YELLOW)Chưa đọc được Kafka/MinIO.$(RESET)"

stream-test: ## Unit test watermark, membership và DLQ, không cần cụm Flink
	PYTHONPATH=. $(PYTHON) -m unittest discover -s tests/streaming -v
