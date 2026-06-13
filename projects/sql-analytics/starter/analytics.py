# Databricks notebook source
# SQL Analytics — Starter
# Complete each TODO using Spark SQL window functions, cohort analysis, and QUALIFY

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
import random
from datetime import date, timedelta

# COMMAND ----------

# 1. Generate sample dataset
random.seed(42)

channels = ["organic", "paid_search", "email", "referral", "social"]
regions  = ["us-west", "us-east", "eu-west", "apac"]
categories = ["electronics", "apparel", "home_goods", "sports"]

products_data = [
    ("prod-001", "Laptop Pro",      "electronics", 999.00),
    ("prod-002", "Wireless Mouse",  "electronics",  29.99),
    ("prod-003", "USB-C Hub",       "electronics",  49.99),
    ("prod-004", "Running Shoes",   "sports",       89.99),
    ("prod-005", "Yoga Mat",        "sports",       34.99),
    ("prod-006", "T-Shirt",         "apparel",      19.99),
    ("prod-007", "Jeans",           "apparel",      59.99),
    ("prod-008", "Coffee Maker",    "home_goods",  129.00),
    ("prod-009", "Desk Lamp",       "home_goods",   45.00),
    ("prod-010", "Notebook",        "apparel",       9.99),
]

customers_data = []
for i in range(1, 101):
    signup = date(2023, random.randint(1, 6), random.randint(1, 28))
    cohort = date(signup.year, signup.month, 1)
    customers_data.append((
        f"cust-{i:04d}",
        f"Customer {i}",
        signup,
        cohort,
        random.choice(channels)
    ))

orders_data = []
oid = 1
for cust_id, _, signup_date, _, channel in customers_data:
    n_orders = random.randint(1, 8)
    for _ in range(n_orders):
        days_after = random.randint(0, 180)
        order_date = signup_date + timedelta(days=days_after)
        product = random.choice(products_data)
        qty = random.randint(1, 3)
        orders_data.append((
            f"ord-{oid:05d}",
            cust_id,
            product[0],
            round(product[3] * qty, 2),
            order_date,
            channel,
            random.choice(regions),
            "completed"
        ))
        oid += 1

# COMMAND ----------

# 2. Create temp views
products_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("name",       StringType(), True),
    StructField("category",   StringType(), True),
    StructField("price",      DoubleType(), True),
])

customers_schema = StructType([
    StructField("customer_id",  StringType(), True),
    StructField("name",         StringType(), True),
    StructField("signup_date",  DateType(),   True),
    StructField("cohort_month", DateType(),   True),
    StructField("channel",      StringType(), True),
])

orders_schema = StructType([
    StructField("order_id",   StringType(), True),
    StructField("customer_id",StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("amount",     DoubleType(), True),
    StructField("order_date", DateType(),   True),
    StructField("channel",    StringType(), True),
    StructField("region",     StringType(), True),
    StructField("status",     StringType(), True),
])

spark.createDataFrame(products_data, products_schema).createOrReplaceTempView("products")
spark.createDataFrame(customers_data, customers_schema).createOrReplaceTempView("customers")
spark.createDataFrame(orders_data, orders_schema).createOrReplaceTempView("orders")

print(f"Products: {spark.table('products').count()}")
print(f"Customers: {spark.table('customers').count()}")
print(f"Orders: {spark.table('orders').count()}")

# COMMAND ----------

# --- TODO 1: Top-10 Customers by Revenue (QUALIFY) ---
# Rank customers by total_revenue DESC using DENSE_RANK().
# Use QUALIFY to filter to rank <= 10 without a subquery.
# Include: customer_id, total_revenue, total_orders, rank.

result_1 = spark.sql("""
    -- TODO: complete this query
    SELECT
        customer_id,
        SUM(amount) AS total_revenue,
        COUNT(DISTINCT order_id) AS total_orders,
        DENSE_RANK() OVER (/* TODO: order by total revenue */) AS revenue_rank
    FROM orders
    WHERE status = 'completed'
    GROUP BY customer_id
    QUALIFY /* TODO */
    ORDER BY total_revenue DESC
""")

raise NotImplementedError("TODO 1: Complete the QUALIFY-based top-10 query above")
# result_1.display()

# COMMAND ----------

# --- TODO 2: Month-over-Month Revenue Growth ---
# Show monthly revenue + prior month revenue (LAG) + growth %.
# Formula: (current - prev) / prev * 100.
# Handle the first month (no prior) gracefully with COALESCE or NULLIF.

result_2 = spark.sql("""
    WITH monthly AS (
        SELECT
            DATE_TRUNC('month', order_date) AS month,
            SUM(amount) AS revenue
        FROM orders
        GROUP BY DATE_TRUNC('month', order_date)
    )
    SELECT
        month,
        revenue,
        -- TODO: LAG(revenue) OVER (ORDER BY month) AS prev_revenue
        -- TODO: ROUND((revenue - prev_revenue) / prev_revenue * 100, 1) AS growth_pct
    FROM monthly
    ORDER BY month
""")

raise NotImplementedError("TODO 2: Add LAG and growth_pct to the MoM query")
# result_2.display()

# COMMAND ----------

# --- TODO 3: Cohort Retention Analysis ---
# For each signup cohort (cohort_month), what % of customers placed
# at least one order in each subsequent month (months_since_signup 0, 1, 2, ...)?
#
# Steps:
#   1. Join orders to customers to get cohort_month for each order
#   2. Compute months_since_signup = MONTHS_BETWEEN(order_month, cohort_month)
#   3. Count distinct customers per (cohort_month, months_since_signup)
#   4. Divide by cohort_size from customers table

result_3 = spark.sql("""
    WITH
    -- TODO: cohort_orders CTE — join orders+customers, compute order_month and months_since_signup
    -- TODO: cohort_sizes CTE — count customers per cohort_month
    SELECT
        -- TODO: select columns and compute retention_pct
    FROM cohort_orders co
    JOIN cohort_sizes cs ON co.cohort_month = cs.cohort_month
    GROUP BY -- TODO
    ORDER BY cohort_month, months_since_signup
""")

raise NotImplementedError("TODO 3: Build the cohort retention query")
# result_3.display()

# COMMAND ----------

# --- TODO 4: Channel Attribution with Running Totals ---
# Monthly revenue per channel + cumulative running total per channel +
# % of that month's total revenue for each channel.

result_4 = spark.sql("""
    WITH channel_monthly AS (
        SELECT
            channel,
            DATE_TRUNC('month', order_date) AS month,
            SUM(amount) AS monthly_revenue
        FROM orders
        GROUP BY channel, DATE_TRUNC('month', order_date)
    )
    SELECT
        channel,
        month,
        monthly_revenue,
        -- TODO: SUM(monthly_revenue) OVER (PARTITION BY channel ORDER BY month) AS running_total
        -- TODO: ROUND(monthly_revenue / SUM(monthly_revenue) OVER (PARTITION BY month) * 100, 1) AS pct_of_monthly_total
    FROM channel_monthly
    ORDER BY month, channel
""")

raise NotImplementedError("TODO 4: Add running_total and pct_of_monthly_total window expressions")
# result_4.display()

# COMMAND ----------

# --- TODO 5: Latest Order per Customer (QUALIFY dedup) ---
# One row per customer — their most recent order.
# Use ROW_NUMBER() + QUALIFY. Tiebreak on order_id DESC.

result_5 = spark.sql("""
    SELECT
        customer_id,
        order_id,
        order_date,
        amount,
        channel,
        ROW_NUMBER() OVER (/* TODO: partition by customer, order by date DESC, order_id DESC */) AS rn
    FROM orders
    QUALIFY rn = 1
    ORDER BY customer_id
""")

raise NotImplementedError("TODO 5: Complete the ROW_NUMBER window spec for QUALIFY dedup")
# result_5.display()

# COMMAND ----------

# --- TODO 6: Revenue by Category PIVOT ---
# Pivot product categories into columns showing monthly revenue.
# Join orders to products first to get category.

result_6 = spark.sql("""
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
        -- TODO: SUM(amount) FOR category IN ('electronics', 'apparel', 'home_goods', 'sports')
    )
    ORDER BY month
""")

raise NotImplementedError("TODO 6: Complete the PIVOT expression")
# result_6.display()

# COMMAND ----------

# --- TODO 7: EXPLAIN the cohort query ---
# Run EXPLAIN FORMATTED on your TODO 3 solution.
# Document:
#   - How many Exchange (shuffle) nodes are in the plan?
#   - Is the customers join a broadcast or sort-merge?
#   - Does predicate pushdown appear?

# spark.sql("EXPLAIN FORMATTED <your TODO 3 query>")

raise NotImplementedError("TODO 7: Run EXPLAIN FORMATTED on the cohort retention query")

# COMMAND ----------

# --- TODO 8: Add BROADCAST hint to cohort query ---
# If the customers table is small (it is — 100 rows), add /*+ BROADCAST(c) */ hint.
# Re-run EXPLAIN FORMATTED. Confirm BroadcastHashJoin appears.
# Time both versions: how much faster with the hint?

raise NotImplementedError("TODO 8: Add BROADCAST hint and compare execution plans")
