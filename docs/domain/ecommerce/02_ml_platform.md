# E-commerce - ML Platform Sample Design

## ML Use Case

**Task:** predict whether a customer will place an order in the next 7 days.

**Why this makes sense:** the business wants to target promotional offers at customers who are likely to buy soon. Sending an offer to someone already certain to buy wastes budget; sending one to someone unlikely to buy is also wasteful. A 7-day purchase prediction lets the marketing team act with a short enough horizon to be relevant.

**Prediction entity:** customer_id

**Prediction time:** the model is evaluated daily. For each active customer, we predict using their feature snapshot as of the previous day.

---

## Label Definition

**Label column:** `will_order_in_7d` (1 = placed at least one order in the next 7 days, 0 = did not)

**How it is derived:**
- Take the orders table, group by customer_id and order_ts.
- For each customer on each day, look forward 7 days and check whether any order exists.
- The label table has one row per customer per day: customer_id, label_date, will_order_in_7d.

**Label table grain:** one row per customer per day.

**Join key:** customer_id + label_date joins to feature snapshots at the same date (event_timestamp).

---

## Feature Plan

**Offline features** (computed from historical orders and order_items, stored as daily snapshots):

| Feature | Description |
|---|---|
| orders_last_30d | number of orders placed in the last 30 days |
| orders_last_90d | number of orders placed in the last 90 days |
| avg_order_value_90d | average order value over 90 days |
| days_since_last_order | days elapsed since the most recent order |
| favorite_category | category with the most order_items in the last 90 days |
| coupon_usage_rate_90d | fraction of orders that used a coupon in the last 90 days |

**Streaming features** (computed from clickstream, updated in near real-time):

| Feature | Description |
|---|---|
| sessions_last_7d | number of browsing sessions in the last 7 days |
| products_viewed_last_7d | number of distinct products viewed in the last 7 days |
| cart_adds_last_7d | number of add-to-cart events in the last 7 days |

---

## Training Data Shape

- Join the label table to the offline feature snapshot on (customer_id, label_date = event_timestamp).
- Join the streaming feature snapshot on the same keys.
- One row per customer per day, with label and all features present.
- Drop rows where key features are null (customers with no order history).

**Split strategy:** time-based split. Train on the first 80% of dates, validate on the next 10%, test on the last 10%. This avoids leaking future behavior into training, which would happen with a random split on time-series data.

---

## Model Choice

**Baseline:** Logistic Regression - simple, fast to train, interpretable coefficients.

**Main model:** Gradient Boosted Trees (e.g. XGBoost or LightGBM) - handles mixed feature types and non-linear relationships well, which are both present here (numeric order counts + categorical favorite_category).

**Evaluation metric:** AUC-ROC, because the label is imbalanced (most days a customer does not order) and we care about ranking customers by likelihood rather than a hard threshold.
