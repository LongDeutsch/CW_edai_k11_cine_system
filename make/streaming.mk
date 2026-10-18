# ==============================================================================
# make/streaming.mk: Flink heartbeat windows, DLQ và Iceberg sink
# ==============================================================================

STREAM_COMPOSE := $(DOCKER_COMPOSE) -f infra/compose/docker-compose.streaming.yml
STREAM_PY := PYTHONPATH=. $(PYTHON)

##@ 6. Streaming Flink (Layer 4)

.PHONY: stream-up stream-down stream-checkpoints stream-job-approve stream-job-run stream-job-stop stream-verify stream-test

stream-up: ## Bật JobManager và TaskManager trên mạng cinestream_default
	$(STREAM_COMPOSE) up -d --build

stream-down: ## Tắt cụm Flink, giữ volume flink_checkpoints. Không đụng MinIO/Kafka
	$(STREAM_COMPOSE) down

stream-checkpoints: ## Liệt kê checkpoint trên volume flink_checkpoints
	$(STREAM_COMPOSE) exec -T taskmanager find /opt/flink/checkpoints -maxdepth 2 -type d

stream-job-approve: ## Chặn submit production nếu chưa có CINESTREAM_ALLOW_STREAMING=true
	@test "$(CINESTREAM_ALLOW_STREAMING)" = "true" || { echo "CINESTREAM_ALLOW_STREAMING chưa bật. Không submit job production."; exit 1; }

stream-job-run: stream-job-approve stream-up ## Submit job production. Cần CINESTREAM_ALLOW_STREAMING=true
	$(STREAM_COMPOSE) exec -T -e CINESTREAM_ALLOW_STREAMING=true jobmanager flink run -d \
		-py /opt/cinestream/streaming/flink/jobs/heartbeat_window_job.py \
		--engine flink \
		--bootstrap kafka:29092 \
		--iceberg-rest http://iceberg-rest:8181 \
		--s3-endpoint http://minio:9000

stream-job-stop: ## Hủy job heartbeat đang chạy. Không đổi offset Kafka
	PYTHONPATH=. $(PYTHON) -m streaming.flink.job_control

stream-verify: ## Chứng minh late data vào DLQ; đọc topic và bucket nếu cụm đang chạy
	@echo -e "$(BOLD)$(BLUE)Watermark + cửa sổ trên chuỗi timestamp lộn xộn$(RESET)"
	$(STREAM_PY) -m streaming.flink.jobs.heartbeat_window_job --engine local --scenario proof
	@echo -e "$(BOLD)$(BLUE)Topic DLQ và metadata Silver (bỏ qua nếu dịch vụ tắt)$(RESET)"
	@PYTHONPATH=. $(UV) run --extra kafka --extra minio python -m streaming.flink.verify || echo -e "$(YELLOW)Chưa đọc được Kafka/MinIO.$(RESET)"

stream-test: ## Unit test watermark, membership và DLQ, không cần cụm Flink
	PYTHONPATH=. $(PYTHON) -m unittest discover -s tests/streaming -v
