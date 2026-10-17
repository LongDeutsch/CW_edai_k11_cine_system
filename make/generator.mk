# ==============================================================================
# make/generator.mk: Các lệnh điều khiển Layer 1 (Data Generator & Tiêm lỗi)
# ==============================================================================

# Tham số mặc định cho Offline Generator (Có thể ghi đè qua CLI)
SCALE_FACTOR ?= 0.1
SKEW_RATIO   ?= 0.8
DUP_RATE     ?= 0.025
RANDOM_SEED  ?= 42
OFFLINE_OUT  ?= $(OUTPUT_BASE_DIR)/scale_$(subst .,_,$(SCALE_FACTOR))

# Tham số mặc định cho Streaming Producer
STREAM_EVENTS ?= 20
LATE_RATIO    ?= 0.06
DUP_RATIO     ?= 0.018
KAFKA_BROKER  ?= localhost:19092
KAFKA_TOPIC   ?= movie.playback.heartbeat

# Tham số mặc định cho MinIO Source Sync
MINIO_ENDPOINT ?= http://localhost:9000
MINIO_BUCKET   ?= external-source-lake

##@ 1. Data Generator (Layer 1)

.PHONY: gen-offline gen-offline-scale-1 stream-dry stream-kafka minio-sync-dry minio-sync verify-generator

gen-offline: ## Sinh dữ liệu Parquet offline nhỏ (0.1% ~27MB) với Skew & Duplicates
	@echo -e "$(BOLD)$(BLUE)[1/1] Chạy Offline Generator (Scale Factor: $(SCALE_FACTOR))...$(RESET)"
	$(PYTHON) -m generator.offline_generator \
		--scale-factor $(SCALE_FACTOR) \
		--skew-ratio $(SKEW_RATIO) \
		--duplicate-rate $(DUP_RATE) \
		--random-seed $(RANDOM_SEED) \
		--output-dir $(OFFLINE_OUT)

gen-offline-scale-1: ## Sinh dữ liệu Parquet mức trung bình (1% ~270MB)
	@echo -e "$(BOLD)$(BLUE)[1/1] Chạy Offline Generator mức 1% (~270MB)...$(RESET)"
	$(PYTHON) -m generator.offline_generator \
		--scale-factor 1.0 \
		--allow-large-scale \
		--skew-ratio $(SKEW_RATIO) \
		--duplicate-rate $(DUP_RATE) \
		--random-seed $(RANDOM_SEED) \
		--output-dir $(OUTPUT_BASE_DIR)/scale_1_0

stream-dry: ## Giả lập phát sự kiện streaming ra Terminal (Dry-Run, không cần Kafka)
	@echo -e "$(BOLD)$(BLUE)[1/1] Phát $(STREAM_EVENTS) sự kiện heartbeat ra console (Late: $(LATE_RATIO), Dup: $(DUP_RATIO))...$(RESET)"
	$(PYTHON) -m generator.streaming_producer \
		--dry-run \
		--max-events $(STREAM_EVENTS) \
		--preview $(STREAM_EVENTS) \
		--late-ratio $(LATE_RATIO) \
		--dup-ratio $(DUP_RATIO) \
		--random-seed $(RANDOM_SEED)

stream-kafka: ## Bắn sự kiện streaming thật vào cụm Kafka (yêu cầu Kafka đang chạy)
	@echo -e "$(BOLD)$(BLUE)[1/1] Đang đẩy streaming events vào Kafka broker $(KAFKA_BROKER)...$(RESET)"
	$(RUN) --extra kafka python -m generator.streaming_producer \
		--kafka-bootstrap $(KAFKA_BROKER) \
		--topic $(KAFKA_TOPIC) \
		--max-events $(STREAM_EVENTS) \
		--late-ratio $(LATE_RATIO) \
		--dup-ratio $(DUP_RATIO)

minio-sync-dry: ## Kiểm tra danh mục ánh xạ file lên MinIO nguồn (Dry-Run)
	@echo -e "$(BOLD)$(BLUE)[1/1] Quét và ánh xạ file Parquet sang MinIO S3 (Dry-Run)...$(RESET)"
	$(PYTHON) -m generator.upload_to_source_minio --dry-run --source-dir $(OFFLINE_OUT)

minio-sync: ## Tải toàn bộ file Parquet lên MinIO bucket s3://external-source-lake
	@echo -e "$(BOLD)$(BLUE)[1/1] Đang tải file lên MinIO $(MINIO_ENDPOINT) bucket $(MINIO_BUCKET)...$(RESET)"
	$(RUN) --extra minio python -m generator.upload_to_source_minio \
		--source-dir $(OFFLINE_OUT) \
		--endpoint $(MINIO_ENDPOINT) \
		--bucket $(MINIO_BUCKET)

verify-generator: ## Kiểm chứng nhanh các tiêu chí Rubric: File count, Schema Evolution, Skew
	@echo -e "$(BOLD)$(BLUE)=== KIỂM CHỨNG KẾT QUẢ DATA GENERATOR ===$(RESET)"
	@$(PYTHON) -c "import os, pyarrow.parquet as pq; \
		p1 = '$(OFFLINE_OUT)/movies/partition_00001.parquet'; \
		p45 = '$(OFFLINE_OUT)/movies/partition_00045.parquet'; \
		print('• File partition_00001 tồn tại:', os.path.exists(p1)); \
		print('• File partition_00045 tồn tại:', os.path.exists(p45)); \
		s1 = len(pq.read_schema(p1)) if os.path.exists(p1) else 0; \
		s45 = len(pq.read_schema(p45)) if os.path.exists(p45) else 0; \
		print(f'• Schema Evolution: File 1 có {s1} cột, File 45 có {s45} cột (Đã tiêm 3 cột mới)'); \
		total_files = sum(len(files) for _, _, files in os.walk('$(OFFLINE_OUT)')); \
		print(f'• Tổng số file Parquet đã tạo: {total_files} files')"
