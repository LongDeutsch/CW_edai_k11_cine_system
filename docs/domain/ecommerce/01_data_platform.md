# E-commerce - Data Platform Sample Design

## Domain Overview

A mid-size online retailer selling electronics and fashion. Customers browse products, place orders, and pay online. The business runs a website and a mobile app. Data comes from two sources: a transactional database (orders, products, customers) and event tracking (clickstream, payment gateway callbacks).

---

## Offline Data

These tables represent historical and reference data. They are generated in bulk and stored as Parquet files.

| Table | Grain | Key Columns |
|---|---|---|
| customers | one row per customer | customer_id, signup_ts, city, segment, device_type |
| products | one row per product | product_id, category, brand, base_price, is_active, created_ts |
| orders | one row per order | order_id, customer_id, order_ts, status, shipping_city, coupon_code |
| order_items | one row per line item | order_item_id, order_id, product_id, quantity, unit_price, discount_amount |
| payments | one row per payment attempt | payment_id, order_id, payment_ts, method, amount, status |

Target volume: >= 100GB across all tables combined.

---

## Streaming Data

These are real-time events produced continuously and consumed by a stream processor.

| Event | Grain | Key Columns |
|---|---|---|
| clickstream | one row per page view | session_id, customer_id, product_id, page_type, event_ts, device_type |
| payment_callbacks | one row per gateway callback | payment_id, order_id, callback_ts, result_code, provider |

---

## Required Data Problems

The rubric defines which problems must be injected. Below is how each one maps to this domain.

**Skew**
- 80% of orders ship to Ho Chi Minh City (shipping_city column in orders).
- 75% of products belong to the electronics category (category column in products).
- Effect: any join or aggregation on shipping_city or category will be skewed.

**Schema evolution**
- Mid-dataset, the payment provider adds a new column: `failure_reason` (string, nullable).
- Earlier payment records do not have this column. The Parquet schema changes at a known offset.
- Effect: any reader must handle files with different schemas in the same dataset.

**Duplicates (offline)**
- ~2% of order_items rows are duplicated due to a retry bug in the order service.
- Duplicates share the same order_item_id but have slightly different created_ts values.
- Effect: aggregations (e.g. total revenue) will overcount unless deduplication is applied.

**Late arrivals (streaming)**
- Mobile app clickstream events arrive 5-10 minutes after the event actually happened.
- The delay is caused by the app batching events and sending them when connectivity is restored.
- Effect: a 1-minute processing window will miss these events unless late-data handling is configured.

**Duplicates (streaming)**
- ~1.5% of payment_callbacks are sent twice by the gateway (at-least-once delivery).
- Duplicates share the same payment_id and result_code.
- Effect: counts and status updates will be wrong unless idempotent processing is applied.

**Additional problems (suggested)**
- High-cardinality join: order_items joined to products on product_id where product catalog has millions of rows.
- Out-of-order events: clickstream events from the same session arrive in non-chronological order.
