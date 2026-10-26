# ==============================================================================
# make/infra.mk: Các lệnh điều khiển hạ tầng Docker Compose (MinIO, Kafka, Redis)
# ==============================================================================

COMPOSE_FILE ?= infra/compose/docker-compose.yml

##@ 2. Hạ tầng & Dịch vụ (Docker Compose)

.PHONY: up down restart ps logs urls demo-flow datahub-up datahub-down

up: ## Khởi động MinIO + Kafka và chờ healthcheck (Console :9001, Kafka UI :8085)
	@if [ -f "$(COMPOSE_FILE)" ]; then \
		echo -e "$(BOLD)$(GREEN)Khởi động containers từ $(COMPOSE_FILE)...$(RESET)"; \
		$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) up -d --wait minio kafka kafka-ui iceberg-rest; \
		$(MAKE) urls; \
	else \
		echo -e "$(YELLOW)[Thông báo] Chưa tìm thấy $(COMPOSE_FILE). Sẽ được dựng ở Layer Hạ tầng.$(RESET)"; \
	fi

down: ## Dừng và xóa toàn bộ containers của cụm hạ tầng
	@if [ -f "$(COMPOSE_FILE)" ]; then \
		echo -e "$(BOLD)$(YELLOW)Dừng cụm containers...$(RESET)"; \
		$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) down; \
	else \
		echo -e "$(YELLOW)[Thông báo] File compose chưa được cấu hình.$(RESET)"; \
	fi

restart: down up ## Khởi động lại toàn bộ cụm hạ tầng

ps: ## Xem trạng thái sức khỏe các containers đang chạy
	@if [ -f "$(COMPOSE_FILE)" ]; then \
		$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) ps; \
	else \
		echo -e "$(YELLOW)[Thông báo] Cụm hạ tầng chưa được bật.$(RESET)"; \
	fi

logs: ## Theo dõi logs thời gian thực của các dịch vụ (Ctrl+C để thoát)
	@if [ -f "$(COMPOSE_FILE)" ]; then \
		$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) logs -f; \
	fi

urls: ## In địa chỉ UI MinIO và Kafka sau khi cụm đã lên
	@echo ""
	@echo -e "$(BOLD)$(CYAN)Giao diện xem dữ liệu$(RESET)"
	@echo -e "  MinIO Console  $(BOLD)http://localhost:9001$(RESET)   minioadmin / minioadmin"
	@echo -e "  Kafka UI       $(BOLD)http://localhost:8085$(RESET)   topic $(KAFKA_TOPIC)"
	@echo -e "  MinIO API      $(BOLD)$(MINIO_ENDPOINT)$(RESET)   bucket $(MINIO_BUCKET)"
	@echo -e "  Kafka broker   $(BOLD)$(KAFKA_BROKER)$(RESET)"
	@echo ""

DATAHUB_COMPOSE := $(DOCKER_COMPOSE) -f infra/compose/docker-compose.datahub.yml

datahub-up: ## Bật DataHub trên mạng cinestream_default. UI :9002, GMS :18083
	$(DATAHUB_COMPOSE) up -d

datahub-down: ## Tắt DataHub. Không đụng MinIO, Kafka heartbeat hay Iceberg
	$(DATAHUB_COMPOSE) down

demo-flow: up ## Sinh Parquet nhỏ, đẩy lên MinIO, bắn heartbeat vào Kafka, rồi in link UI
	@if [ ! -d "$(OFFLINE_OUT)/ratings" ]; then \
		echo -e "$(BOLD)$(BLUE)Chưa có Parquet tại $(OFFLINE_OUT). Đang sinh scale 0.1...$(RESET)"; \
		$(MAKE) gen-offline; \
	else \
		echo -e "$(GREEN)Dùng Parquet có sẵn: $(OFFLINE_OUT)$(RESET)"; \
	fi
	$(MAKE) minio-sync
	$(MAKE) stream-kafka STREAM_EVENTS=100
	@echo ""
	@echo -e "$(BOLD)$(GREEN)Đã đẩy file Parquet và 100 heartbeat.$(RESET)"
	@echo -e "1. Mở MinIO Console, bucket $(BOLD)$(MINIO_BUCKET)$(RESET), xem users/, movies/, ratings/dt=..."
	@echo -e "2. Mở Kafka UI, topic $(BOLD)$(KAFKA_TOPIC)$(RESET), tab Messages."
	$(MAKE) urls
