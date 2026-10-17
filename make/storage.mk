# ==============================================================================
# make/storage.mk: Layer 2 — Lakehouse Iceberg, SCD2, compaction, UI
# ==============================================================================

LAKEHOUSE_UI_PORT ?= 8088
LAKEHOUSE_COMPOSE ?= infra/compose/docker-compose.lakehouse.yml

##@ 5. Lakehouse Storage (Layer 2)

.PHONY: lakehouse-check scd2-demo compact-demo test-scd2 lakehouse-ui lakehouse-docker demo-layer2

lakehouse-check: ## Liệt kê bảng Bronze / Silver / Gold đã khai báo trong DDL
	@echo -e "$(BOLD)$(BLUE)Ba zone Medallion$(RESET)"
	@$(PYTHON) -c "from storage.lakehouse.catalog import TABLES; \
zones = ('bronze', 'silver', 'gold'); \
[print(f'  {t.zone:8} {t.name}') for t in TABLES]"

scd2-demo: ## In bảng dim_users trước và sau khi 5 user đổi gói
	@echo -e "$(BOLD)$(BLUE)SCD Type 2 trên 5 user$(RESET)"
	$(PYTHON) -m storage.lakehouse.scd.apply_scd2

compact-demo: ## Mô phỏng gom 16 file nhỏ thành file 128MB
	@echo -e "$(BOLD)$(BLUE)Bin-packing mục tiêu 128MB$(RESET)"
	$(PYTHON) -m storage.lakehouse.maintenance.compaction --target-mb 128

test-scd2: ## Chạy test SCD2 và in bảng trước/sau
	@echo -e "$(BOLD)$(BLUE)Test logic SCD2$(RESET)"
	$(PYTHON) tests/unit/test_scd2_logic.py

lakehouse-ui: ## Mở trang xem luồng tại http://127.0.0.1:8088 (Ctrl+C để dừng)
	@echo -e "$(BOLD)$(GREEN)Mở http://127.0.0.1:$(LAKEHOUSE_UI_PORT)$(RESET)"
	$(PYTHON) -m storage.lakehouse.ui.server --host 127.0.0.1 --port $(LAKEHOUSE_UI_PORT)

lakehouse-docker: ## Build image và chạy cùng trang UI trong Docker, cổng 8088
	@echo -e "$(BOLD)$(BLUE)Build UI Lakehouse...$(RESET)"
	$(DOCKER_COMPOSE) -f $(LAKEHOUSE_COMPOSE) up --build -d
	@echo -e "$(BOLD)$(GREEN)UI trong container: http://127.0.0.1:$(LAKEHOUSE_UI_PORT)$(RESET)"

demo-layer2: lakehouse-check test-scd2 compact-demo ## Kiểm chứng Layer 2 trên Terminal, không cần Docker
	@echo ""
	@echo -e "$(BOLD)$(GREEN)=================================================================$(RESET)"
	@echo -e "$(BOLD)$(GREEN)   ✓ LAYER 2: DDL, SCD2 VÀ COMPACTION ĐÃ CHẠY TRÊN MẪU NHỎ   $(RESET)"
	@echo -e "$(BOLD)$(GREEN)=================================================================$(RESET)"
	@echo -e "Báo cáo số liệu tại:        $(BOLD)docs/proof/02_lakehouse_storage_proof.md$(RESET)"
	@echo -e "Xem luồng trên trình duyệt: $(BOLD)make lakehouse-ui$(RESET)"
	@echo -e "Hoặc trong Docker:          $(BOLD)make lakehouse-docker$(RESET)"
	@echo ""
