# ==============================================================================
# make/llm.mk: Ollama local. Tên model lấy từ env hoặc infra/compose/.env.example.
# Lệnh `OLLAMA_MODEL=... make llm-pull` thắng file env. Compose không ghim model.
# ==============================================================================

OLLAMA_COMPOSE = $(DOCKER_COMPOSE) -f infra/compose/docker-compose.ollama.yml

define ollama_env
saved_model="$${OLLAMA_MODEL-}"; \
saved_base="$${OLLAMA_BASE_URL-}"; \
saved_embed="$${OLLAMA_EMBED_MODEL-}"; \
set -a; \
. infra/compose/.env.example; \
if [ -f infra/compose/.env ]; then . infra/compose/.env; fi; \
set +a; \
if [ -n "$$saved_model" ]; then OLLAMA_MODEL="$$saved_model"; fi; \
if [ -n "$$saved_base" ]; then OLLAMA_BASE_URL="$$saved_base"; fi; \
if [ -n "$$saved_embed" ]; then OLLAMA_EMBED_MODEL="$$saved_embed"; fi; \
if [ -z "$${OLLAMA_MODEL}" ] || [ -z "$${OLLAMA_BASE_URL}" ]; then \
  echo "thiếu OLLAMA_MODEL hoặc OLLAMA_BASE_URL"; \
  exit 1; \
fi
endef

##@ 7. LLM & AI Agent (FastMCP Concierge)

.PHONY: llm-up llm-pull llm-check llm-test mcp-serve agent-chat concierge-test

llm-up: ## Bật Ollama :11434. Không pull model
	$(OLLAMA_COMPOSE) up -d --wait

llm-pull: llm-up ## Pull đúng OLLAMA_MODEL vào volume ollama_models
	@$(ollama_env); \
	$(OLLAMA_COMPOSE) exec -T ollama ollama pull "$$OLLAMA_MODEL"

llm-check: ## Hỏi một câu và in tên model server trả về
	@$(ollama_env); \
	PYTHONPATH=. $(PYTHON) -m llm.client

llm-test: ## Client gửi tên từ env, không gọi server
	PYTHONPATH=. $(PYTHON) -m unittest tests.llm.test_client -v

mcp-serve: ## Khởi chạy CineBot Concierge Server (FastMCP & SSE Stream) trên cổng :18088
	@echo -e "$(BOLD)$(GREEN)Khởi chạy CineBot Concierge Server (:18088)...$(RESET)"
	PYTHONPATH=. $(PYTHON) -m llm.server

# Bắt các tham số tự do truyền sau make agent-chat
ifeq (agent-chat,$(firstword $(MAKECMDGOALS)))
  CHAT_ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
  $(eval $(CHAT_ARGS):;@:)
endif

agent-chat: ## Chat thử nghiệm với CineBot Agent (vd: make agent-chat MSG="..." hoặc make agent-chat "câu hỏi")
	@echo -e "$(BOLD)$(CYAN)--- CineBot Agent Chat Session ---$(RESET)"
	@MSG_ARG="$(strip $(if $(MSG),$(MSG),$(if $(CHAT_ARGS),$(CHAT_ARGS),Gợi ý cho tôi phim hành động hay)))"; \
	PYTHONPATH=. $(PYTHON) -c "from llm.agent import agent; import sys; msg = sys.argv[1]; res = agent.run_flow(msg, user_id=1); print('=== Intent:', res['intent'], '==='); print(res['reply'])" "$$MSG_ARG"

concierge-test: ## Kiểm thử toàn diện FastMCP Tools, CineBot Agent và SSE Streaming
	@echo -e "$(BOLD)$(BLUE)Chạy kiểm thử FastMCP, Agent & SSE Stream...$(RESET)"
	PYTHONPATH=. $(PYTHON) -m unittest tests.llm.test_mcp_tools tests.llm.test_agent tests.llm.test_sse_stream -v
