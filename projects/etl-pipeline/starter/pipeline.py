# Databricks notebook source
# ETL Pipeline — Starter
# Bronze → Silver → Gold medallion architecture
# Fill in each TODO. Run cells top-to-bottom in a Databricks notebook.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import date, datetime

# COMMAND ----------

# Bronze schema — all strings, accept anything
BRONZE_SCHEMA = StructType([
    StructField("event_id",    StringType(), nullable=True),
    StructField("order_id",    StringType(), nullable=True),
    StructField("customer_id", StringType(), nullable=True),
    StructField("product_id",  StringType(), nullable=True),
    StructField("quantity",    StringType(), nullable=True),
    StructField("unit_price",  StringType(), nullable=True),
    StructField("status",      StringType(), nullable=True),
    StructField("region",      StringType(), nullable=True),
    StructField("event_ts",    StringType(), nullable=True),
])

# Sample raw events — note: evt-003 duplicates evt-001, evt-006 updates ord-1002 status
RAW_EVENTS = [
    ("evt-001", "ord-1001", "cust-A", "prod-X", "2",  "49.99",  "completed", "us-west", "2024-01-15 08:00:00"),
    ("evt-002", "ord-1002", "cust-B", "prod-Y", "1",  "129.00", "completed", "us-east", "2024-01-15 09:00:00"),
    ("evt-003", "ord-1001", "cust-A", "prod-X", "2",  "49.99",  "completed", "us-west", "2024-01-15 08:00:00"),  # duplicate event
    ("evt-004", "ord-1003", "cust-C", "prod-X", "3",  "49.99",  "cancelled", "eu-west", "2024-01-15 10:00:00"),
    ("evt-005", "ord-1004", "cust-D", "prod-Z", "1",  None,     "pending",   "us-west", "2024-01-15 11:00:00"),  # null price
    ("evt-006", "ord-1002", "cust-B", "prod-Y", "1",  "129.00", "shipped",   "us-east", "2024-01-15 14:00:00"),  # status update
]

# COMMAND ----------

# --- SETUP: Create catalog/schemas (run once) ---
spark.sql("CREATE CATALOG IF NOT EXISTS pipeline_demo")
spark.sql("CREATE SCHEMA IF NOT EXISTS pipeline_demo.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS pipeline_demo.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS pipeline_demo.gold")
spark.sql("CREATE SCHEMA IF NOT EXISTS pipeline_demo.monitoring")

# COMMAND ----------

# --- STEP 1: Create target tables ---

spark.sql("""
    CREATE TABLE IF NOT EXISTS pipeline_demo.bronze.orders_raw (
        event_id    STRING,
        order_id    STRING,
        customer_id STRING,
        product_id  STRING,
        quantity    STRING,
        unit_price  STRING,
        status      STRING,
        region      STRING,
        event_ts    STRING,
        ingestion_ts TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS pipeline_demo.silver.orders (
        order_id    STRING      NOT NULL,
        customer_id STRING      NOT NULL,
        product_id  STRING      NOT NULL,
        quantity    INT         NOT NULL,
        unit_price  DECIMAL(10,2) NOT NULL,
        amount      DECIMAL(10,2) NOT NULL,
        status      STRING      NOT NULL,
        region      STRING      NOT NULL,
        order_date  DATE        NOT NULL,
        ingested_at TIMESTAMP   NOT NULL
    )
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS pipeline_demo.gold.revenue_daily (
        order_date      DATE,
        product_id      STRING,
        region          STRING,
        total_orders    BIGINT,
        total_revenue   DECIMAL(14,2),
        avg_order_value DECIMAL(10,2)
    )
    USING DELTA
""")

# COMMAND ----------

# --- TODO 1: Bronze Ingestion ---
# Create a DataFrame from RAW_EVENTS using BRONZE_SCHEMA.
# Deduplicate on event_id (same event sent twice by the source system).
# Add an ingestion_ts column with current_timestamp().
# Append to pipeline_demo.bronze.orders_raw.
# Hint: spark.createDataFrame(RAW_EVENTS, BRONZE_SCHEMA)
# Hint: .dropDuplicates(["event_id"])
# Hint: .withColumn("ingestion_ts", current_timestamp())
# Hint: .write.format("delta").mode("append").saveAsTable(...)

raise NotImplementedError("TODO 1: Implement bronze ingestion with deduplication")

# COMMAND ----------

# Verify bronze (should have 5 rows — evt-003 is a duplicate of evt-001 on event_id)
print(f"Bronze row count: {spark.table('pipeline_demo.bronze.orders_raw').count()}")
spark.table("pipeline_demo.bronze.orders_raw").display()

# COMMAND ----------

# --- TODO 2: Silver Transformation ---
# Read from bronze. Apply type casting and validation:
#   - Cast quantity to IntegerType
#   - Cast unit_price to DecimalType(10, 2)
#   - Compute amount = quantity * unit_price, cast to DecimalType(10, 2)
#   - Cast event_ts to TimestampType then extract order_date as DateType
#   - Add ingested_at = current_timestamp()
#   - FILTER OUT rows where unit_price IS NULL or quantity IS NULL
# Deduplicate on order_id — keep the row with the LATEST event_ts
#   (ord-1002 was updated to 'shipped' — keep the later event)
# Hint: use window function: Window.partitionBy("order_id").orderBy(desc("event_ts"))
#       then filter to row_number == 1

raise NotImplementedError("TODO 2: Read bronze, cast types, compute amount, dedup by order_id keeping latest")

# COMMAND ----------

# --- TODO 3: Silver MERGE ---
# MERGE the silver_df into pipeline_demo.silver.orders on order_id.
# When matched: update status and ingested_at.
# When not matched: insert all columns.
# Hint: from delta.tables import DeltaTable
#       DeltaTable.forName(spark, "pipeline_demo.silver.orders")
#       .alias("target").merge(silver_df.alias("source"), "target.order_id = source.order_id")
#       .whenMatchedUpdate(set={...})
#       .whenNotMatchedInsertAll()
#       .execute()

raise NotImplementedError("TODO 3: MERGE silver_df into silver.orders")

# COMMAND ----------

# Verify silver (should have 5 rows — ord-1004 excluded for null price, ord-1002 updated to 'shipped')
print(f"Silver row count: {spark.table('pipeline_demo.silver.orders').count()}")
spark.table("pipeline_demo.silver.orders").orderBy("order_id").display()

# COMMAND ----------

# --- TODO 4: Gold Aggregation ---
# Read silver. Filter to status IN ('completed', 'shipped').
# Group by order_date, product_id, region.
# Compute: total_orders (count), total_revenue (sum of amount), avg_order_value (avg of amount).
# MERGE into gold on (order_date + product_id + region) — update all metrics when matched.
# Hint: merge condition: "target.order_date = source.order_date AND target.product_id = source.product_id AND target.region = source.region"

raise NotImplementedError("TODO 4: Aggregate to gold and MERGE")

# COMMAND ----------

# Verify gold
print(f"Gold row count: {spark.table('pipeline_demo.gold.revenue_daily').count()}")
spark.table("pipeline_demo.gold.revenue_daily").display()

# COMMAND ----------

# --- TODO 5: Validation Queries ---
# Run these three checks and confirm the expected results:
# 1. Layer row counts: bronze=5, silver=5, gold=?
# 2. Null check on silver.amount: should be 0
# 3. Silver status for ord-1002 should be 'shipped' (latest event wins)

raise NotImplementedError("TODO 5: Write and run the three validation queries above")

# COMMAND ----------

# --- TODO 6: Time Travel ---
# Read pipeline_demo.bronze.orders_raw as of VERSION 0.
# How many rows were in the first write?
# Hint: spark.read.format("delta").option("versionAsOf", 0).table(...)

raise NotImplementedError("TODO 6: Read bronze table at version 0 and count rows")

# COMMAND ----------

# --- TODO 7: OPTIMIZE the silver table ---
# Run OPTIMIZE on pipeline_demo.silver.orders, Z-ordered by (order_date, region).
# Hint: spark.sql("OPTIMIZE pipeline_demo.silver.orders ZORDER BY (order_date, region)")

raise NotImplementedError("TODO 7: OPTIMIZE silver with Z-order")

# COMMAND ----------

# --- TODO 8: Inspect transaction history ---
# Show the full history of pipeline_demo.silver.orders.
# Identify all operation types that appear (WRITE? MERGE? OPTIMIZE?).
# Hint: spark.sql("DESCRIBE HISTORY pipeline_demo.silver.orders")

raise NotImplementedError("TODO 8: Show silver table history")

# COMMAND ----------

# --- TODO 9: Set retention policy ---
# Set log retention to 30 days on silver.orders.
# Hint: ALTER TABLE ... SET TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 30 days')

raise NotImplementedError("TODO 9: Set 30-day retention on silver.orders")
