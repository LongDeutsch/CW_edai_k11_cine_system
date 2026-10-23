# ==============================================================================
# make/spark.mk: Các lệnh điều khiển Apache Spark & Flink Jobs (Layer 3 & 4)
# PySpark 3.5 cần Python 3.12. uv tạo môi trường riêng, không đụng .venv 3.13.
# ==============================================================================

# Local: data_generator/output/scale_0_1
# MinIO: s3a://external-source-lake  (cần make up && make minio-sync)
SPARK_SRC ?= data_generator/output/scale_0_1
# Iceberg REST: s3a://lakehouse  (catalog http://127.0.0.1:8181)
# Parquet local: data_generator/output/lakehouse
SPARK_WAREHOUSE ?= s3a://lakehouse
SPARK_HOLD ?= 0
SPARK_RUN_ID ?= manual
SPARK_EFFECTIVE_TS ?= 2026-03-01T00:00:00+00:00
SPARK_PYTHON ?= 3.12
SPARK_PY = SPARK_LOCAL_IP=127.0.0.1 PYTHONUNBUFFERED=1 PYTHONPATH=. $(UV) run --no-project --python $(SPARK_PYTHON) --with pyspark==3.5.5 python

##@ 4. Xử lý Dữ liệu Phân tán (Spark & Flink)

.PHONY: spark-baseline spark-optimized spark-aqe spark-build-gold spark-ingest-bronze spark-publish spark-batch-it spark-schema-it demo-layer3 flink-job

spark-baseline: ## Join lệch khóa, không mergeSchema. SPARK_HOLD=120 để xem UI :4040
	@echo -e "$(BOLD)$(BLUE)Khởi chạy Spark Baseline Job...$(RESET)"
	$(SPARK_PY) -m transformation.batch.baseline.unoptimized_join \
		--source-dir $(SPARK_SRC) \
		--hold-seconds $(SPARK_HOLD)

spark-optimized: ## Salting + mergeSchema + row_number + broadcast thể loại
	@echo -e "$(BOLD)$(BLUE)Khởi chạy Spark Optimized Job (Salting + MergeSchema)...$(RESET)"
	$(SPARK_PY) -m transformation.batch.optimized.handle_skew_salting \
		--source-dir $(SPARK_SRC) \
		--hold-seconds $(SPARK_HOLD)

spark-aqe: ## Tự động chia nhỏ skewed partition bằng Spark Adaptive Query Execution (AQE)
	@echo -e "$(BOLD)$(BLUE)Khởi chạy Spark AQE Skew Join Job (AQE Adaptive)...$(RESET)"
	$(SPARK_PY) -m transformation.batch.optimized.handle_skew_aqe \
		--source-dir $(SPARK_SRC) \
		--hold-seconds $(SPARK_HOLD)

spark-build-gold: ## Ghi Silver, dim_users SCD2, fact_ratings, fact_watch_sessions, OBT từ landing
	@echo -e "$(BOLD)$(BLUE)Dựng Silver và Gold từ $(SPARK_SRC)...$(RESET)"
	$(SPARK_PY) -m transformation.batch.jobs.build_gold_layer \
		--source-dir $(SPARK_SRC) \
		--warehouse $(SPARK_WAREHOUSE) \
		--hold-seconds $(SPARK_HOLD)

spark-ingest-bronze: ## Parquet nguồn → Bronze Iceberg. SPARK_RUN_ID không tham gia khóa chống trùng
	@echo -e "$(BOLD)$(BLUE)Ingest $(SPARK_SRC) vào namespace bronze...$(RESET)"
	$(SPARK_PY) -m transformation.batch.jobs.ingest_to_bronze \
		--source-dir $(SPARK_SRC) \
		--warehouse $(SPARK_WAREHOUSE) \
		--namespace bronze \
		--run-id $(SPARK_RUN_ID)

spark-publish: ## Bronze → Silver → Gold. SPARK_EFFECTIVE_TS là mốc đổi gói SCD2
	@echo -e "$(BOLD)$(BLUE)Publish Bronze sang Silver và Gold...$(RESET)"
	$(SPARK_PY) -m transformation.batch.jobs.publish_silver_gold \
		--warehouse $(SPARK_WAREHOUSE) \
		--effective-ts $(SPARK_EFFECTIVE_TS) \
		--enable-aqe \
		--no-ui

spark-batch-it: ## Integration test batch trên warehouse file:// cách ly
	@echo -e "$(BOLD)$(BLUE)Chạy integration test batch lakehouse...$(RESET)"
	$(SPARK_PY) -m transformation.batch.it_phase1

spark-schema-it: ## So schema và partition trên warehouse file://, không ghi production
	@echo -e "$(BOLD)$(BLUE)Chạy kiểm tra hợp đồng schema...$(RESET)"
	$(SPARK_PY) -m transformation.batch.it_schema_contract

demo-layer3: ## So sánh 3 phương pháp: Baseline vs Salting vs AQE trên local[*]
	@echo -e "$(BOLD)$(BLUE)So sánh Baseline vs Salting vs AQE trên local[*]...$(RESET)"
	$(SPARK_PY) -m transformation.batch.demo_layer3 --source-dir $(SPARK_SRC) --no-ui

flink-job: stream-job-run ## Submit job Flink heartbeat (xem make/streaming.mk)
