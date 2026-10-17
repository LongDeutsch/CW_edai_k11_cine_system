# Engineering for Data and AI - Coursework Proposal

## 1. Objective

Each group (num. of students = 1) will design and implement an end-to-end Data + AI system in one domain (for example: e-commerce, finance, logistics, healthcare, education, IoT).

An example of how to formulate the problem for the e-commerce domain is in [domain/ecommerce](domain/ecommerce).

---

## 2. Module Structure

The course has two modules and you build one single system across both. You pick one domain and one dataset, then extend it progressively through Module 1 and Module 2.

This mirrors how real-world AI systems are built: a shared data foundation that serves both classical ML and generative AI use cases in the same product.

---

## 3. Domain Proposal

Before writing any code, you need to define your domain. A good domain proposal answers three questions.

**What data exists in your domain?**

List the main entities and events. For each one, name the key columns (IDs, timestamps, measures, categories) and the grain (what one row represents). Think about what data would realistically come from a transactional system, a user activity log, or a sensor feed.

Example (e-commerce): an `orders` table (one row per order, columns: order_id, customer_id, order_timestamp, status, shipping_city), an `order_items` table (one row per line item), a `clickstream` event stream (one event per page view with session_id and product_id).

**How do the required problems show up in your domain?**

The rubric defines specific data problems you must generate and solve. Your job is to think about how each problem makes sense in your domain - which columns are skewed, what schema change is realistic, which events arrive late and why. If you can think of additional problems beyond the required ones, add them.

Example (e-commerce): 80% of orders come from one city (geographic skew); a payment provider adds a new `failure_reason` column mid-stream (schema evolution); clickstream events from mobile apps arrive 5-10 minutes late.

**What AI problem will you solve?**

Define the prediction or generation task your AI system targets. It should be grounded in your domain data and use both offline and streaming features.

Example (ML): predict whether a customer will purchase in the next session, using historical order features and real-time clickstream features.

Example (LLM/Agent): an agent that answers customer service questions by retrieving product and order information, then calling an ML model to predict likely issues.

---

## 4. What You Will Build

By the end of the course you will have one working system covering three layers.

**Module 1 - Layer 1: Data Platform**

The infrastructure that stores and moves data: a lakehouse (bronze/silver/gold zones), a data warehouse, a batch processing cluster (Spark), a stream processing cluster (Flink), a pipeline orchestrator (Airflow), and a data governance layer (DataHub). Everything runs on this platform - pipelines, jobs, and downstream systems are built on top of it.

**Module 1 - Layer 2: ML Platform**

The infrastructure that enables machine learning: a feature store (Feast), a pipeline runner for training (Kubeflow), an experiment tracker and model registry (MLflow), and a model serving layer (KServe). Data scientists and engineers use this platform to build and deploy ML models without managing the underlying infrastructure themselves.

**Module 2 - Layer 3: LLM/Agent Platform**

The infrastructure that runs LLM-powered agents: an LLM inference server, an agent gateway (MCP and A2A), and an agent registry. Agents and MCP tools are deployed on top of this platform and use the feature store and ML models from Layer 2 as part of their tool calls.
