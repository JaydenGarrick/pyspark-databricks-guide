# Databricks notebook source
# SQL Analytics — Finished Solution

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
import random
from datetime import date, timedelta

# COMMAND ----------

# 1. Generate sample dataset (identical seed to starter — same data)
random.seed(42)

channels   = ["organic", "paid_search", "email", "referral", "social"]
regions    = ["us-west", "us-east", "eu-west", "apac"]

products_data = [
    ("prod-001", "Laptop Pro",     "electronics", 999.00),
    ("prod-002", "Wireless Mouse", "electronics",  29.99),
    ("prod-003", "USB-C Hub",      "electronics",  49.99),
    ("prod-004", "Running Shoes",  "sports",       89.99),
    ("prod-005", "Yoga Mat",       "sports",       34.99),
    ("prod-006", "T-Shirt",        "apparel",      19.99),
    ("prod-007", "Jeans",          "apparel",      59.99),
    ("prod-008", "Coffee Maker",   "home_goods",  129.00),
    ("prod-009", "Desk Lamp",      "home_goods",   45.00),
    ("prod-010", "Notebook",       "apparel",       9.99),
]

customers_data = []
for i in range(1, 101):
    signup = date(2023, random.randint(1, 6), random.randint(1, 28))
    cohort = date(signup.year, signup.month, 1)
    customers_data.append((f"cust-{i:04d}", f"Customer {i}", signup, cohort, random.choice(channels)))

orders_data = []
oid = 1
for cust_id, _, signup_date, _, channel in customers_data:
    for _ in range(random.randint(1, 8)):
        order_date = signup_date + timedelta(days=random.randint(0, 180))
        product = random.choice(products_data)
        qty = random.randint(1, 3)
        orders_data.append((f"ord-{oid:05d}", cust_id, product[0], round(product[3] * qty, 2),
                             order_date, channel, random.choice(regions), "completed"))
        oid += 1

products_schema = StructType([StructField("product_id", StringType()), StructField("name", StringType()),
                               StructField("category", StringType()), StructField("price", DoubleType())])
customers_schema = StructType([StructField("customer_id", StringType()), StructField("name", StringType()),
                                StructField("signup_date", DateType()), StructField("cohort_month", DateType()),
                                StructField("channel", StringType())])
orders_schema = StructType([StructField("order_id", StringType()), StructField("customer_id", StringType()),
                             StructField("product_id", StringType()), StructField("amount", DoubleType()),
                             StructField("order_date", DateType()), StructField("channel", StringType()),
                             StructField("region", StringType()), StructField("status", StringType())])

spark.createDataFrame(products_data, products_schema).createOrReplaceTempView("products")
spark.createDataFrame(customers_data, customers_schema).createOrReplaceTempView("customers")
spark.createDataFrame(orders_data, orders_schema).createOrReplaceTempView("orders")

# COMMAND ----------

# 2. Top-10 customers by revenue using QUALIFY
# DENSE_RANK allows ties; QUALIFY avoids a subquery
spark.sql("""
    SELECT
        customer_id,
        ROUND(SUM(amount), 2)       AS total_revenue,
        COUNT(DISTINCT order_id)    AS total_orders,
        DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS revenue_rank
    FROM orders
    WHERE status = 'completed'
    GROUP BY customer_id
    QUALIFY revenue_rank <= 10
    ORDER BY total_revenue DESC
""").display()

# COMMAND ----------

# 3. Month-over-Month revenue growth with LAG
# NULLIF prevents division-by-zero on first month
spark.sql("""
    WITH monthly AS (
        SELECT
            DATE_TRUNC('month', order_date)  AS month,
            ROUND(SUM(amount), 2)            AS revenue
        FROM orders
        GROUP BY DATE_TRUNC('month', order_date)
    )
    SELECT
        month,
        revenue,
        LAG(revenue) OVER (ORDER BY month)                                          AS prev_revenue,
        ROUND(
            (revenue - LAG(revenue) OVER (ORDER BY month))
            / NULLIF(LAG(revenue) OVER (ORDER BY month), 0) * 100,
        1)                                                                           AS growth_pct
    FROM monthly
    ORDER BY month
""").display()

# COMMAND ----------

# 4. Cohort retention — % of each cohort still ordering in each subsequent month
spark.sql("""
    WITH cohort_orders AS (
        SELECT
            c.customer_id,
            c.cohort_month,
            DATE_TRUNC('month', o.order_date)                           AS order_month,
            CAST(MONTHS_BETWEEN(
                DATE_TRUNC('month', o.order_date), c.cohort_month
            ) AS INT)                                                   AS months_since_signup
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        WHERE months_since_signup >= 0
    ),
    cohort_sizes AS (
        SELECT cohort_month, COUNT(DISTINCT customer_id) AS cohort_size
        FROM customers
        GROUP BY cohort_month
    )
    SELECT
        co.cohort_month,
        co.months_since_signup,
        COUNT(DISTINCT co.customer_id)                                  AS customers_active,
        cs.cohort_size,
        ROUND(COUNT(DISTINCT co.customer_id) * 100.0 / cs.cohort_size, 1) AS retention_pct
    FROM cohort_orders co
    JOIN cohort_sizes cs ON co.cohort_month = cs.cohort_month
    GROUP BY co.cohort_month, co.months_since_signup, cs.cohort_size
    ORDER BY cohort_month, months_since_signup
""").display()

# COMMAND ----------

# 5. Channel attribution: monthly revenue + cumulative running total + % of total that month
spark.sql("""
    WITH channel_monthly AS (
        SELECT
            channel,
            DATE_TRUNC('month', order_date) AS month,
            ROUND(SUM(amount), 2)           AS monthly_revenue
        FROM orders
        GROUP BY channel, DATE_TRUNC('month', order_date)
    )
    SELECT
        channel,
        month,
        monthly_revenue,
        SUM(monthly_revenue) OVER (PARTITION BY channel ORDER BY month
                                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
        ROUND(
            monthly_revenue / SUM(monthly_revenue) OVER (PARTITION BY month) * 100,
        1)                                                                             AS pct_of_monthly_total
    FROM channel_monthly
    ORDER BY month, channel
""").display()

# COMMAND ----------

# 6. Latest order per customer — QUALIFY dedup, no subquery
spark.sql("""
    SELECT
        customer_id,
        order_id,
        order_date,
        ROUND(amount, 2) AS amount,
        channel,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC, order_id DESC) AS rn
    FROM orders
    QUALIFY rn = 1
    ORDER BY customer_id
""").display()

# COMMAND ----------

# 7. Revenue PIVOT by product category
spark.sql("""
    SELECT *
    FROM (
        SELECT
            DATE_TRUNC('month', o.order_date) AS month,
            p.category,
            o.amount
        FROM orders o
        JOIN products p ON o.product_id = p.product_id
    )
    PIVOT (
        ROUND(SUM(amount), 2)
        FOR category IN ('electronics', 'apparel', 'home_goods', 'sports')
    )
    ORDER BY month
""").display()

# COMMAND ----------

# 8. EXPLAIN the cohort retention query — how many shuffles? what join type?
spark.sql("""EXPLAIN FORMATTED
    WITH cohort_orders AS (
        SELECT c.customer_id, c.cohort_month,
               DATE_TRUNC('month', o.order_date) AS order_month,
               CAST(MONTHS_BETWEEN(DATE_TRUNC('month', o.order_date), c.cohort_month) AS INT) AS months_since_signup
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
    ),
    cohort_sizes AS (
        SELECT cohort_month, COUNT(DISTINCT customer_id) AS cohort_size FROM customers GROUP BY cohort_month
    )
    SELECT co.cohort_month, co.months_since_signup, COUNT(DISTINCT co.customer_id) AS customers_active,
           ROUND(COUNT(DISTINCT co.customer_id) * 100.0 / cs.cohort_size, 1) AS retention_pct
    FROM cohort_orders co JOIN cohort_sizes cs ON co.cohort_month = cs.cohort_month
    WHERE months_since_signup >= 0
    GROUP BY co.cohort_month, co.months_since_signup, cs.cohort_size
""")

# COMMAND ----------

# 9. BROADCAST hint — customers is 100 rows, always safe to broadcast
spark.sql("""EXPLAIN FORMATTED
    WITH cohort_orders AS (
        SELECT /*+ BROADCAST(c) */ c.customer_id, c.cohort_month,
               DATE_TRUNC('month', o.order_date) AS order_month,
               CAST(MONTHS_BETWEEN(DATE_TRUNC('month', o.order_date), c.cohort_month) AS INT) AS months_since_signup
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
    ),
    cohort_sizes AS (
        SELECT cohort_month, COUNT(DISTINCT customer_id) AS cohort_size FROM customers GROUP BY cohort_month
    )
    SELECT co.cohort_month, co.months_since_signup, COUNT(DISTINCT co.customer_id) AS customers_active,
           ROUND(COUNT(DISTINCT co.customer_id) * 100.0 / cs.cohort_size, 1) AS retention_pct
    FROM cohort_orders co JOIN cohort_sizes cs ON co.cohort_month = cs.cohort_month
    WHERE months_since_signup >= 0
    GROUP BY co.cohort_month, co.months_since_signup, cs.cohort_size
""")
# With BROADCAST(c): plan shows BroadcastHashJoin — no shuffle on customers side
