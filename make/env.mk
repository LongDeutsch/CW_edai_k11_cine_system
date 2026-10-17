# ==============================================================================
# make/env.mk: Biến môi trường toàn cục, màu sắc Terminal & Trình tạo Help tự động
# ==============================================================================

# Thiết lập Shell chuẩn Bash
SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c

# Định dạng màu sắc Terminal (ANSI Colors)
BOLD   := \033[1m
GREEN  := \033[32m
YELLOW := \033[33m
BLUE   := \033[34m
CYAN   := \033[36m
RESET  := \033[0m

# Python của project là môi trường uv (.venv), không phải python3 trên PATH.
UV ?= uv
RUN = $(UV) run
PYTHON = $(RUN) python
DOCKER_COMPOSE ?= docker compose

# Thư mục gốc dữ liệu
SEED_DIR ?= data_generator/data_offline/ml-latest-small
OUTPUT_BASE_DIR ?= data_generator/output

# Tự động sinh giao diện Help từ các comment có tiền tố ##@ (Nhóm) và ## (Mô tả lệnh)
.PHONY: help
help: ## Hiển thị menu hướng dẫn các lệnh
	@echo ""
	@echo -e "$(BOLD)$(CYAN)=================================================================$(RESET)"
	@echo -e "$(BOLD)$(CYAN)    CineStream Data Platform - Quản lý quy trình bằng Makefile   $(RESET)"
	@echo -e "$(BOLD)$(CYAN)=================================================================$(RESET)"
	@echo -e "Sử dụng: $(BOLD)make [target] [THAM_SỐ=giá_trị]$(RESET)"
	@awk 'BEGIN {FS = ":.*?## "} \
		/^##@/ { printf "\n$(BOLD)$(YELLOW)%s$(RESET)\n", substr($$0, 5) } \
		/^[a-zA-Z0-9_-]+:.*?## / { printf "  $(BOLD)$(GREEN)%-24s$(RESET) %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""
