# E-commerce - LLM/Agent Platform Sample Design

## Agent Use Case

**Agent:** a customer service assistant that handles post-purchase questions.

**Why this makes sense:** customers frequently ask questions like "where is my order", "why was my payment declined", and "will I like this product". Answering these requires looking up live order data, retrieving product information, and predicting customer behavior - tasks that combine structured data retrieval and generation. A human agent would need to check multiple systems; this agent does it in one conversation.

---

## Knowledge Base

The RAG pipeline indexes the following sources:

| Source | What it contains | Chunking approach |
|---|---|---|
| Product catalog | product descriptions, specifications, return policy per category | one chunk per product, split long descriptions at 512 tokens |
| FAQ documents | common questions about shipping, returns, payments | one chunk per question-answer pair |
| Policy documents | return policy, warranty terms, shipping SLA by region | split by section heading |

**Why these sources:** customers ask about products and policies most often. Order data is structured and fetched directly from the feature store, not indexed in the knowledge base, because it changes per customer and per order.

---

## Tool Plan

The agent has two tools. It decides which to call based on the question type.

**Tool 1 - data fetch (MCP tool)**
- Fetches the customer's recent orders, current order status, and real-time features (sessions, cart activity) from the online feature store.
- Used when: the question is about a specific order or the customer's current activity (e.g. "where is my order", "what did I last view").
- Input: customer_id
- Output: structured order and feature data as JSON

**Tool 2 - ML inference**
- Calls the purchase prediction model from the ML platform.
- Used when: the agent needs to personalize a response based on predicted behavior (e.g. deciding whether to offer a discount to keep the customer).
- Input: customer_id (the serving API fetches features internally)
- Output: probability score + recommendation label

The agent uses RAG on every turn to retrieve relevant product or policy context before generating a response.

---

## Example Interaction

**User:** "My order #4821 hasn't arrived yet, it's been 5 days. What's happening?"

**Agent reasoning:**
1. Call Tool 1 (data fetch) with the customer's customer_id to get order #4821 status and shipping details.
2. Retrieve relevant chunks from the knowledge base: shipping SLA by region, delay policy.
3. Call Tool 2 (ML inference) to get the purchase likelihood score - if the customer is high-risk for churn, the agent may offer a voucher.
4. Generate a response that combines the live order status, the policy context, and optionally the voucher offer.

**Agent response (example):** "Order #4821 is currently at the sorting facility in Da Nang and is expected to arrive within 1-2 days. Our standard shipping SLA for your region is 5-7 business days, so this is within the expected window. I have added a 50k VND voucher to your account for the inconvenience."
