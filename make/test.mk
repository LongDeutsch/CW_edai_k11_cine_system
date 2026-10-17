# ==============================================================================
# make/test.mk: Kiểm thử, đo độ bao phủ code & Dọn dẹp cache môi trường
# ==============================================================================

##@ 3. Kiểm thử & Chất lượng mã nguồn (Quality & Clean)

.PHONY: test lint clean-cache clean-data clean

test: ## Chạy toàn bộ Unit Tests với báo cáo kiểm tra lỗi
	@echo -e "$(BOLD)$(BLUE)Đang chạy kiểm thử Unit Tests...$(RESET)"
	$(PYTHON) -m pytest tests/unit/ -v || echo -e "$(YELLOW)[Thông báo] Thư mục tests/unit đang được chuẩn bị.$(RESET)"

lint: ## Kiểm tra định dạng code bằng Ruff hoặc Flake8
	@echo -e "$(BOLD)$(BLUE)Đang kiểm tra chất lượng mã nguồn (Linter)...$(RESET)"
	@if $(RUN) ruff --version >/dev/null 2>&1; then \
		$(RUN) ruff check generator/; \
	else \
		echo -e "$(YELLOW)[Gợi ý] Thêm ruff vào môi trường uv: uv add --dev ruff$(RESET)"; \
	fi

clean-cache: ## Xóa sạch file biên dịch tạm Python (__pycache__, .pytest_cache)
	@echo -e "$(BOLD)$(YELLOW)Dọn dẹp Python bytecode cache...$(RESET)"
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@rm -rf .pytest_cache .coverage htmlcov 2>/dev/null || true
	@echo -e "$(GREEN)✓ Đã dọn sạch cache Python.$(RESET)"

clean-data: ## Xóa dữ liệu output đã sinh trong data_generator/output/
	@echo -e "$(BOLD)$(YELLOW)Xóa dữ liệu sinh tạm thời...$(RESET)"
	@rm -rf $(OUTPUT_BASE_DIR)/scale_* 2>/dev/null || true
	@echo -e "$(GREEN)✓ Đã dọn sạch thư mục output.$(RESET)"

clean: clean-cache clean-data ## Dọn dẹp toàn bộ dữ liệu tạm và cache hệ thống
