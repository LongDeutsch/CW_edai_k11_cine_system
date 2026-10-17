# Movie Recommendation - LLM/Agent Platform Architecture & Design

## 1. Agent Use Case

**Tên Tác tử (Agent):** Cinephile AI Assistant & Movie Concierge (**"CineBot"**).

**Ý nghĩa bài toán nghiệp vụ:**
- Người dùng dịch vụ phát phim trực tuyến thường gặp phải hội chứng "Analysis Paralysis" (choáng ngợp trước hàng ngàn tựa phim và mất trung bình 15-20 phút lướt màn hình mà không chọn được phim).
- Các thuật toán đề xuất truyền thống chỉ có thể hiển thị danh sách tĩnh dựa trên ID phim mà không thể giải thích: *"Tại sao phim này hợp với bạn?"* hoặc tiếp nhận những câu hỏi tự nhiên mang tính cảm xúc phức tạp, ví dụ:
  - *"Tìm cho tôi một bộ phim trinh thám ly kỳ có cú twist bất ngờ kiểu Inception, nhưng phát hành sau năm 2018 và không có cảnh quá bạo lực để xem cùng gia đình tối nay."*
  - *"Bộ phim Oppenheimer có độ dài bao nhiêu? Tôi đã xem hết Interstellar và Dunkirk rồi thì có thích phim này không?"*
- Để trả lời trọn vẹn, hệ thống đòi hỏi sự kết hợp đồng thời của 3 năng lực:
  1. **Truy xuất thông tin ngữ nghĩa phi cấu trúc (RAG):** Tìm kiếm nội dung tóm tắt cốt truyện, thông điệp, đánh giá phê bình và cảnh báo phụ huynh (parental guides).
  2. **Truy vấn trạng thái người dùng (Data Fetch Tool via MCP):** Lấy lịch sử phim đã xem và đặc trưng thời gian thực từ Feature Store để tránh gợi ý phim người dùng đã xem rồi.
  3. **Chấm điểm đề xuất cá nhân hóa (ML Inference Tool):** Gọi mô hình LightGBM/Two-Tower trên KServe để dự đoán mức độ yêu thích cụ thể của người dùng này với các ứng viên vừa tìm được.

---

## 2. Knowledge Base & RAG Pipeline

Cơ sở tri thức (Knowledge Base) phi cấu trúc được lập chỉ mục (indexing) vào Vector Database (Qdrant / Milvus) thông qua một RAG Data Pipeline điều phối bởi Airflow:

| Nguồn dữ liệu (Source) | Nội dung chi tiết | Chiến lược phân đoạn (Chunking) | Metadata đính kèm |
|---|---|---|---|
| **Movie Synopses & Plot Outlines** | Tóm tắt chi tiết cốt truyện, bối cảnh phim, chủ đề tư tưởng. | 512 tokens / chunk, overlap 50 tokens (để giữ trọn mạch cảm xúc và cú twist). | `movie_id`, `release_year`, `director`, `genres` |
| **Parental & Content Advisories** | Chi tiết cảnh báo độ tuổi (bạo lực, ngôn từ, cảnh nhạy cảm, độ ghê rợn). | 1 chunk cho toàn bộ mục cảnh báo của mỗi phim. | `movie_id`, `content_rating` (PG-13, R, etc.) |
| **Critical Reviews & Trivia** | Các bài bình luận chuyên môn, thông tin hậu trường, giải thưởng đạt được. | Phân đoạn theo từng đoạn văn đánh giá chuyên gia (~256 tokens). | `movie_id`, `source`, `sentiment_score` |
| **FAQ & Policy Docs** | Quy định đổi gói cước thuê bao, chính sách chia sẻ tài khoản, tải phim offline. | 1 chunk cho mỗi cặp Question - Answer. | `category`, `topic` |

**Quy trình RAG Ingestion Pipeline:**
1. Trích xuất văn bản từ cơ sở dữ liệu phim và file Markdown tài liệu.
2. Sinh vector embedding sử dụng mô hình mã hóa đa ngôn ngữ: `text-embedding-3-small` hoặc mô hình mã nguồn mở `bge-large-en-v1.5`.
3. Tải và lập chỉ mục HNSW vào Qdrant Vector Database, hỗ trợ tìm kiếm kết hợp giữa vector tương đồng và bộ lọc metadata (Hybrid Search: Dense Vector + Sparse BM25 + Scalar Filtering theo `release_year` và `genres`).

---

## 3. Tool Plan (Model Context Protocol - MCP)

Agent tương tác với thế giới bên ngoài thông qua các công cụ chuẩn hóa theo giao thức **Model Context Protocol (MCP)** do Anthropic khởi xướng, chạy trên cụm Kubernetes:

```
                  ┌──────────────────────────────────────────────┐
                  │           Agent Gateway (MCP & A2A)          │
                  │  (Rate Limiting, Auth, Policy, Sandboxing)   │
                  └───────────────────────┬──────────────────────┘
                                          │
        ┌─────────────────────────────────┼────────────────────────────────┐
        ▼                                 ▼                                ▼
[Tool 1: MCP Data Fetch]       [Tool 2: ML Inference]          [Tool 3: Vector RAG]
- Gọi Feast Online Store       - Gọi KServe API                - Gọi Qdrant Hybrid Search
- Input: user_id               - Input: user_id, movie_ids     - Input: query_text, filters
- Output: watch history,       - Output: ranking scores,       - Output: top-k movie contexts,
  favorite genres, state         win probability                 synopsis & advisories
```

### Tool 1: `fetch_user_movie_profile` (MCP Tool)
- **Mô tả:** Truy xuất lịch sử xem phim gần đây, các thể loại yêu thích, các phim đã chấm điểm cao, và trạng thái thời gian thực (đặc trưng streaming từ Redis).
- **Khi nào gọi:** Khi người dùng bắt đầu cuộc trò chuyện hoặc yêu cầu gợi ý phim mang tính cá nhân.
- **Tham số đầu vào:** `user_id: str`, `limit_recent: int = 10`.
- **Dữ liệu trả về (JSON):** Danh sách các `movie_id` đã xem, điểm đánh giá trung bình, các thể loại tương tác nhiều nhất trong 24h qua.

### Tool 2: `predict_movie_ranking` (ML Inference Tool)
- **Mô tả:** Gửi danh sách các ứng viên phim (`candidate_movie_ids`) kèm `user_id` tới dịch vụ KServe Model Serving để nhận về điểm số xác suất yêu thích (`will_watch_score`).
- **Khi nào gọi:** Sau khi Agent tìm được một danh sách các phim phù hợp với nội dung người dùng mô tả và cần sắp xếp lại thứ tự từ cao xuống thấp để giới thiệu.
- **Tham số đầu vào:** `user_id: str`, `candidate_movie_ids: list[str]`.
- **Dữ liệu trả về (JSON):** Danh sách các cặp `[{"movie_id": "m102", "predicted_score": 0.89}, ...]`.

### Tool 3: `search_movie_knowledge_base` (RAG Search Tool)
- **Mô tả:** Thực hiện tìm kiếm ngữ nghĩa trên cơ sở dữ liệu Vector Qdrant để tìm các bộ phim có cốt truyện, sắc thái cảm xúc hoặc bối cảnh phù hợp với truy vấn tự nhiên.
- **Khi nào gọi:** Khi người dùng hỏi về cốt truyện, tìm phim theo mô tả mơ hồ, hoặc hỏi về thông tin cảnh báo độ tuổi.
- **Tham số đầu vào:** `query_text: str`, `genre_filter: optional[str]`, `max_content_rating: optional[str]`.
- **Dữ liệu trả về (JSON):** Đoạn trích tóm tắt phim, tên đạo diễn, dàn diễn viên, và cảnh báo nội dung.

---

## 4. Multi-Agent & Gateway Architecture

Hệ thống được thiết kế theo mô hình điều phối nhiều tác tử (**Multi-Agent System with Coordinator Pattern**) đáp ứng trọn vẹn Rubric EDAI MLE:

```
[End User Web / Mobile Client]
              │
              ▼
    [External Gateway (NGINX)]
              │ (Auth, Rate Limiting, TLS)
              ▼
[Coordinator Agent (CineCoordinator)] ◄─── (Registered on Agent Registry)
              │
              ├── (A2A Gateway with Security & Policy)
              │
              ├──► [Agent 1: CineDataAgent] ────► [Tool 1: Feast MCP Server]
              │
              ├──► [Agent 2: CineRankAgent] ────► [Tool 2: KServe Inference]
              │
              └──► [Agent 3: CineRAGAgent]  ────► [Tool 3: Qdrant Vector Store]
```

1. **LLM Inference Platform:**
   - Triển khai mô hình cục bộ với máy chủ suy luận hiệu năng cao **vLLM** (mô hình `Qwen/Qwen2.5-7B-Instruct` hoặc `meta-llama/Llama-3-8B-Instruct`).
   - Tối ưu hóa PagedAttention và Continuous Batching đạt thông lượng $> 150 \text{ tokens/s}$ với độ trễ phản hồi từ đầu tiên (TTFT) $< 200\text{ms}$.
2. **Agent Gateway (MCP & A2A):**
   - Sử dụng **agentgateway** làm cổng trung gian tập trung (Centralized Proxy) cho toàn bộ các kết nối MCP Tool và trao đổi giữa các Agent (Agent-to-Agent).
   - Thiết lập các chính sách bảo mật: Kiểm tra quyền hạn (Role-based Access Control), giới hạn số lượng gọi (Rate Limiting), khử thông tin nhạy cảm (PII Redaction), và chạy tool execution bên trong môi trường cô lập (Sandbox Environment).
3. **Coordinator Agent:**
   - Đóng vai trò nhạc trưởng tiếp nhận yêu cầu từ người dùng, phân tích ý định (Intent Classification), điều phối tác vụ tới các sub-agents chuyên biệt và tổng hợp câu trả lời cuối cùng.
   - Được đăng ký trên **Agent Registry** kèm siêu dữ liệu và cấu hình hệ thống (System Prompt quản lý trên Langfuse).
4. **Agent Fleet & Warm-up Architecture:**
   - Cài đặt hệ thống ở chế độ **Warm-Up Fleet** (Substrate Worker Pool) để giữ sẵn các tiến trình khởi tạo, giảm thiểu Cold-Start latency từ 8 giây xuống dưới 0.5 giây khi có yêu cầu đột biến.

---

## 5. Example Interaction & Step-by-Step Reasoning Trace

### 5.1. Tình huống trò chuyện (User Prompt)
> **User (ID: `u_9281`):** "Tôi muốn tìm một bộ phim khoa học viễn tưởng du hành không gian có nhiều bí ẩn hack não, tương tự như Interstellar nhưng tôi muốn phim mới ra mắt gần đây (sau 2018). Phim đừng quá bạo lực nhé vì tôi xem cùng con gái 13 tuổi. Bạn gợi ý phim nào hay nhất?"

### 5.2. Chuỗi suy luận và kích hoạt công cụ (Agent Reasoning Trace)

```
[Step 1: Intent Analysis & Plan Generation]
- Thể loại yêu cầu: Sci-Fi, Space Exploration, Mind-bending mystery.
- Điều kiện ràng buộc: release_year >= 2018, content_rating <= PG-13 (an toàn cho lứa tuổi 13).
- User Profile: u_9281. Cần kiểm tra lịch sử xem để không gợi ý phim user đã xem rồi.

[Step 2: Gọi Tool 1 (fetch_user_movie_profile)]
- Request: {"user_id": "u_9281"}
- Response: {
    "recent_watched_movies": ["Interstellar", "The Martian", "Gravity", "Arrival"],
    "favorite_genres": ["Sci-Fi", "Mystery"],
    "disliked_genres": ["Horror"]
  }
=> Nhận xét: User đã xem Arrival và Gravity. Cần loại bỏ 2 phim này khỏi danh sách ứng viên!

[Step 3: Gọi Tool 3 (search_movie_knowledge_base - RAG)]
- Request: {
    "query_text": "space travel exploration mind-bending mystery high concept",
    "genre_filter": "Sci-Fi",
    "max_content_rating": "PG-13",
    "year_min": 2018
  }
- Response: Tìm thấy 3 ứng viên hàng đầu:
    1. "Ad Astra" (2019, PG-13) - Du hành tới rìa hệ mặt trời tìm kiếm cha mất tích và giải mã tín hiệu bí ẩn.
    2. "The Wandering Earth" (2019, PG-13) - Kế hoạch di chuyển Trái Đất khỏi Mặt Trời sắp nổ.
    3. "The Midnight Sky" (2020, PG-13) - Nhà khoa học đơn độc tại Bắc Cực cảnh báo tàu vũ trụ quay về.

[Step 4: Gọi Tool 2 (predict_movie_ranking - ML Inference)]
- Request: {
    "user_id": "u_9281",
    "candidate_movie_ids": ["m_ad_astra_2019", "m_wandering_earth_2019", "m_midnight_sky_2020"]
  }
- Response: [
    {"movie_id": "m_ad_astra_2019", "predicted_score": 0.88, "rank": 1},
    {"movie_id": "m_midnight_sky_2020", "predicted_score": 0.72, "rank": 2},
    {"movie_id": "m_wandering_earth_2019", "predicted_score": 0.65, "rank": 3}
  ]
=> Lựa chọn tối ưu nhất: "Ad Astra (2019)" đứng đầu bảng xếp hạng cá nhân hóa.

[Step 5: Generate Final Personalized Response]
Tổng hợp thông tin cốt truyện, cảnh báo độ tuổi PG-13, liên hệ với sở thích xem phim Interstellar của user, và đưa ra lời giải thích thuyết phục.
```

### 5.3. Câu trả lời của Agent tới người dùng (Agent Response)

> "Chào bạn! Dựa trên sở thích của bạn với những tác phẩm đỉnh cao như *Interstellar* và *Arrival*, tôi xin đề xuất cho bạn bộ phim hoàn hảo nhất cho buổi tối nay cùng con gái:
>
> 🚀 **Bộ phim số 1: Ad Astra (Giải Mã Bí Ẩn Ngân Hà - 2019)**
> - **Điểm tương đồng với Interstellar:** Phim mang bầu không khí kỳ vĩ, trầm mặc về hành trình đơn độc đi qua Mặt Trăng, Sao Hỏa và tiến thẳng tới Sao Hải Vương để điều tra nguồn gốc của những đợt sóng năng lượng đe dọa sự sống loài người.
> - **Yếu tố hack não:** Càng đi sâu vào không gian, nhân vật chính (Brad Pitt thủ vai) càng phải đối mặt với những bí mật chấn động về người cha mất tích nhiều năm trước và bản chất thực sự của sứ mệnh.
> - **Độ tuổi & An toàn:** Phim dán nhãn **PG-13** (rất an toàn cho bạn và con gái 13 tuổi), tập trung vào chiều sâu tâm lý và triết học không gian, không có cảnh bạo lực hay máu me quá mức.
> - **Độ tương thích với bạn:** Thuật toán đề xuất tính toán bộ phim này đạt độ tương thích **88%** với hồ sơ xem phim của bạn!
>
> Nếu xem xong bạn vẫn muốn xem thêm, bộ phim dự phòng số 2 rất đáng thưởng thức là **The Midnight Sky (2020)**. Chúc hai bố con có một buổi tối xem phim tuyệt vời!"
