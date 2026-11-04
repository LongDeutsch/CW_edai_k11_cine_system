# Kiến trúc FastMCP Server & CineBot LLM Concierge

Tài liệu này mô tả chi tiết giải pháp **Giai đoạn 3 (AI Agent & FastMCP Concierge)** cho CineStream:
1. **FastMCP Server**: Cung cấp 3 công cụ (Tools) chuẩn giao thức Model Context Protocol để kết nối hạ tầng dữ liệu và Machine Learning.
2. **CineBot Agent**: Bộ điều phối ReAct Agent phân tích ý định (Intent Recognition), gọi công cụ tự động và tổng hợp câu trả lời tự nhiên.
3. **Server-Sent Events (SSE) Streaming API**: Phục vụ phản hồi trực tiếp theo thời gian thực (Token Streaming) cho ứng dụng người dùng.

---

## 1. Sơ đồ luồng nghiệp vụ (Business Flow Diagrams)

### 1.1. Sơ đồ tuần tự tương tác (Sequence Diagram)

```mermaid
sequenceDiagram
    autonumber
    actor User as Người dùng (Web UI)
    participant SSE as SSE Stream Endpoint (:18088)
    participant Agent as CineBot Agent (ReAct)
    participant MCP as FastMCP Server
    participant Redis as Redis Cache (:16379)
    participant Qdrant as Qdrant Vector DB (:16333)
    participant KServe as ML Serving KServe v2 (:18086)
    participant Ollama as Ollama LLM (:11434)

    User->>SSE: GET/POST /api/chat/stream?message="Gợi ý phim trộm cắp ly kỳ"&user_id=1
    SSE-->>User: event: start {"status": "started"}

    SSE->>Agent: Điều phối ReAct Flow
    Agent->>Agent: Nhận diện Intent: recommendation

    Note over Agent,Redis: BƯỚC 1: LẤY HỒ SƠ NGƯỜI DÙNG
    Agent->>MCP: Gọi Tool: fetch_user_movie_profile(user_id=1)
    MCP->>Redis: Đọc rec:user:1 hoặc hồ sơ lịch sử
    Redis-->>MCP: Trả về {favorite_genre: "Drama|Crime", avg_rating: 4.2}
    MCP-->>Agent: Kết quả hồ sơ người dùng
    SSE-->>User: event: tool_result {"tool": "fetch_user_movie_profile", ...}

    Note over Agent,Qdrant: BƯỚC 2: TÌM KIẾM NGỮ NGHĨA CỐT TRUYỆN
    Agent->>MCP: Gọi Tool: search_movie_knowledge_base("trộm cắp ly kỳ", top_k=5)
    MCP->>Qdrant: Truy vấn HNSW Cosine (768 chiều)
    Qdrant-->>MCP: Trả về Top-5 phim (Heat, City of Lost Children, ...)
    MCP-->>Agent: Danh sách phim ứng viên và tóm tắt plot
    SSE-->>User: event: tool_result {"tool": "search_movie_knowledge_base", "count": 5}

    Note over Agent,KServe: BƯỚC 3: XẾP HẠNG BẰNG MACHINE LEARNING
    Agent->>MCP: Gọi Tool: predict_movie_ranking(user_id=1, candidate_ids=[...])
    MCP->>KServe: POST /recommend/1 hoặc /v2/models/recommendation/infer
    KServe-->>MCP: Trả về xác suất yêu thích: [{id: 6, score: 0.88}, ...]
    MCP-->>Agent: Danh sách xếp hạng giảm dần theo ML score
    SSE-->>User: event: tool_result {"tool": "predict_movie_ranking", ...}

    Note over Agent,Ollama: BƯỚC 4: TỔNG HỢP CÂU TRẢ LỜI TỰ NHIÊN
    Agent->>Ollama: Gửi Prompt kèm Profile + Top phim đã xếp hạng
    Ollama-->>Agent: Sinh văn bản giải thích hấp dẫn

    Note over SSE,User: BƯỚC 5: STREAMING TỪNG TỪ (TOKEN) VỀ UI
    loop Mỗi từ trong câu trả lời
        SSE-->>User: event: token {"token": " Dựa trên"}
        SSE-->>User: event: token {"token": " sở thích"}
    end

    SSE-->>User: event: done {"status": "completed", "recommended_movies": [...]}
```

---

### 1.2. Sơ đồ khối ReAct Flowchart (Decision Flow)

```mermaid
flowchart TD
    Start(["Nhận câu hỏi từ Người dùng"]) --> Detect{"Phân tích Intent"}

    Detect -->|recommendation / search| Step1["1. Gọi Tool: fetch_user_movie_profile<br/>Đọc thể loại yêu thích & lịch sử xem"]
    Detect -->|general_qa| General["Trả lời chào hỏi / Hướng dẫn sử dụng"]

    Step1 --> Step2["2. Gọi Tool: search_movie_knowledge_base<br/>Vector Search HNSW trên Qdrant :16333"]
    Step2 --> Step3["3. Gọi Tool: predict_movie_ranking<br/>Chấm điểm xác suất yêu thích từ ML Serving :18086"]
    Step3 --> Step4["4. Kết hợp Metadata + Điểm ML<br/>Lọc Top-K phim tốt nhất"]
    Step4 --> Synthesis["5. LLM Synthesis (Ollama)<br/>Tạo lời khuyên tự nhiên và giải thích lý do"]

    Synthesis --> StreamSSE["6. Server-Sent Events (SSE)<br/>Đẩy từng Token về giao diện thời gian thực"]
    General --> StreamSSE
    StreamSSE --> End(["Kết thúc phiên tương tác"])
```

---

## 2. Ý nghĩa và chức năng của các module chính

| Module / File | Vai trò kỹ thuật chính | Chi tiết triển khai |
| :--- | :--- | :--- |
| `llm/mcp/fastmcp_compat.py` | Lớp tương thích chuẩn Model Context Protocol (FastMCP) | Cung cấp decorator `@mcp.tool()`, tự động phân tích chữ ký hàm (type annotations) thành JSON Schema chuẩn hóa cho LLM Function Calling. |
| `llm/mcp/server.py` | FastMCP Server đăng ký 3 công cụ cốt lõi | - `fetch_user_movie_profile(user_id)`: Đọc Redis Cache `:16379`.<br/>- `search_movie_knowledge_base(query, top_k)`: Tìm kiếm ngữ nghĩa cốt truyện trên Qdrant `:16333`.<br/>- `predict_movie_ranking(user_id, candidate_ids)`: Gọi ML Serving KServe v2 `:18086`. |
| `llm/agent.py` | CineBot ReAct Orchestrator | Nhận diện ý định hội thoại (intent detection), tự động phối hợp chuỗi công cụ (Chaining: Profile $\rightarrow$ Qdrant $\rightarrow$ ML Ranking), và tổng hợp lời giải thích hấp dẫn. |
| `llm/streaming.py` | Trình phát sinh sự kiện SSE | Sinh chuỗi sự kiện `start`, `tool_start`, `tool_result`, `token`, `done` theo chuẩn `text/event-stream`. |
| `llm/server.py` | Dịch vụ HTTP / SSE Concierge | Khởi chạy máy chủ trên cổng `:18088`. Cung cấp endpoint kiểm tra sức khỏe `/health`, danh mục công cụ `/mcp/tools`, và streaming `/api/chat/stream`. |

---

## 3. Kết quả kiểm thử tự động (Test Pass Evidence)

Tất cả các thành phần của Giai đoạn 3 được kiểm thử tự động toàn diện qua 3 bộ test trong thư mục `tests/llm/`:

### 3.1. Kiểm thử 3 FastMCP Tools (`tests/llm/test_mcp_tools.py`)
```bash
$ PYTHONPATH=. python3 -m unittest tests.llm.test_mcp_tools -v
test_fetch_user_movie_profile (tests.llm.test_mcp_tools.TestFastMCPTools) ... ok
test_mcp_server_tools_registered (tests.llm.test_mcp_tools.TestFastMCPTools) ... ok
test_predict_movie_ranking (tests.llm.test_mcp_tools.TestFastMCPTools) ... ok
test_search_movie_knowledge_base (tests.llm.test_mcp_tools.TestFastMCPTools) ... ok

----------------------------------------------------------------------
Ran 4 tests in 0.012s

OK
```

### 3.2. Kiểm thử CineBot ReAct Agent (`tests/llm/test_agent.py`)
```bash
$ PYTHONPATH=. python3 -m unittest tests.llm.test_agent -v
test_intent_detection (tests.llm.test_agent.TestCineBotAgent) ... ok
test_run_general_qa_flow (tests.llm.test_agent.TestCineBotAgent) ... ok
test_run_recommendation_flow (tests.llm.test_agent.TestCineBotAgent) ... ok

----------------------------------------------------------------------
Ran 3 tests in 0.015s

OK
```

### 3.3. Kiểm thử SSE Streaming & HTTP Server (`tests/llm/test_sse_stream.py`)
```bash
$ PYTHONPATH=. python3 -m unittest tests.llm.test_sse_stream -v
test_concierge_handler_health_and_tools (tests.llm.test_sse_stream.TestSSEStreaming) ... ok
test_concierge_handler_sse_streaming (tests.llm.test_sse_stream.TestSSEStreaming) ... ok
test_stream_agent_events_structure (tests.llm.test_sse_stream.TestSSEStreaming) ... ok

----------------------------------------------------------------------
Ran 3 tests in 0.380s

OK
```

---

## 4. Các lệnh khởi tạo và kiểm thử qua Makefile

Các mục tiêu tiện ích đã được tích hợp sẵn vào [`make/llm.mk`](file:///Users/nguyenbuihoanglong/Desktop/FSDS/edai_11/coursework1/edai-k11-cw/make/llm.mk):

```bash
# 1. Chạy toàn bộ bộ kiểm thử tự động cho Giai đoạn 3 (FastMCP, Agent & SSE)
make concierge-test

# 2. Khởi chạy CineBot Concierge Server (cổng :18088)
make mcp-serve

# 3. Thử nghiệm trò chuyện với CineBot Agent trực tiếp qua dòng lệnh:
make agent-chat
# Hoặc truyền câu hỏi tùy ý:
make agent-chat MSG="Gợi ý cho tôi phim hành động trộm cướp ngân hàng kịch tính"
```
