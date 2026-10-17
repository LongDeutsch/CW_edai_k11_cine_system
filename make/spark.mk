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
SPARK_PYTHON ?= 3.12
SPARK_PY = SPARK_LOCAL_IP=127.0.0.1 PYTHONUNBUFFERED=1 PYTHONPATH=. $(UV) run --no-project --python $(SPARK_PYTHON) --with pyspark==3.5.5 python

##@ 4. Xử lý Dữ liệu Phân tán (Spark & Flink)

.PHONY: spark-baseline spark-optimized spark-aqe spark-build-gold demo-layer3 flink-job

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

spark-build-gold: ## Ghi Silver, dim_users SCD2, fact_ratings, fact_watch_sessions, OBT
	@echo -e "$(BOLD)$(BLUE)Dựng Silver và Gold từ $(SPARK_SRC)...$(RESET)"
	$(SPARK_PY) -m transformation.batch.jobs.build_gold_layer \
		--source-dir $(SPARK_SRC) \
		--warehouse $(SPARK_WAREHOUSE) \
		--hold-seconds $(SPARK_HOLD)

demo-layer3: ## So sánh 3 phương pháp: Baseline vs Salting vs AQE trên local[*]
	@echo -e "$(BOLD)$(BLUE)So sánh Baseline vs Salting vs AQE trên local[*]...$(RESET)"
	$(SPARK_PY) -m transformation.batch.demo_layer3 --source-dir $(SPARK_SRC) --no-ui

flink-job: stream-job-run ## Submit job Flink heartbeat (xem make/streaming.mk)
