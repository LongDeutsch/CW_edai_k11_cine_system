# ==============================================================================
# CineStream Data Platform - Root Makefile
# Mô hình: Makefile gốc + Thư mục make/*.mk mô-đun hóa (Không bị phình code)
# ==============================================================================

# Thiết lập mục tiêu mặc định khi gõ lệnh 'make' mà không truyền tham số
.DEFAULT_GOAL := help

# 1. Nạp biến môi trường toàn cục và trình tạo Help
include make/env.mk

# 2. Nạp các module theo từng Layer chức năng
include make/generator.mk
include make/storage.mk
include make/infra.mk
include make/spark.mk
include make/streaming.mk
include make/test.mk

##@ 0. Luồng công việc tổng hợp (Quickstart Workflows)

.PHONY: demo-layer1 quickstart

demo-layer1: gen-offline verify-generator stream-dry minio-sync-dry ## Chạy trọn gói kiểm thử Layer 1 (Sinh data, Verify, Stream dry-run, MinIO dry-run)
	@echo ""
	@echo -e "$(BOLD)$(GREEN)=================================================================$(RESET)"
	@echo -e "$(BOLD)$(GREEN)   ✓ HOÀN THÀNH KIỂM CHỨNG TOÀN DIỆN LAYER 1 (DATA GENERATOR)   $(RESET)"
	@echo -e "$(BOLD)$(GREEN)=================================================================$(RESET)"
	@echo -e "Dữ liệu offline sẵn sàng tại: $(BOLD)$(OFFLINE_OUT)$(RESET)"
	@echo -e "Báo cáo số liệu chi tiết tại: $(BOLD)docs/proof/01_data_generator_proof.md$(RESET)"
	@echo ""

quickstart: demo-layer1 ## Lệnh nhanh tương đương demo-layer1
