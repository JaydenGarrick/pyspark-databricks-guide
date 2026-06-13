# Databricks notebook source
# ETL Pipeline — Finished Solution
# Bronze → Silver → Gold medallion architecture

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# 1. Schema definitions and sample data
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

RAW_EVENTS = [
    ("evt-001", "ord-1001", "cust-A", "prod-X", "2",  "49.99",  "completed", "us-west", "2024-01-15 08:00:00"),
    ("evt-002", "ord-1002", "cust-B", "prod-Y", "1",  "129.00", "completed", "us-east", "2024-01-15 09:00:00"),
    ("evt-003", "ord-1001", "cust-A", "prod-X", "2",  "49.99",  "completed", "us-west", "2024-01-15 08:00:00"),
    ("evt-004", "ord-1003", "cust-C", "prod-X", "3",  "49.99",  "cancelled", "eu-west", "2024-01-15 10:00:00"),
    ("evt-005", "ord-1004", "cust-D", "prod-Z", "1",  None,     "pending",   "us-west", "2024-01-15 11:00:00"),
    ("evt-006", "ord-1002", "cust-B", "prod-Y", "1",  "129.00", "shipped",   "us-east", "2024-01-15 14:00:00"),
]

# COMMAND ----------

# 2. Create catalog and schemas (idempotent)
spark.sql("CREATE CATALOG IF NOT EXISTS pipeline_demo")
for schema in ["bronze", "silver", "gold", "monitoring"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS pipeline_demo.{schema}")

# COMMAND ----------

# 3. Create target Delta tables
spark.sql("""
    CREATE TABLE IF NOT EXISTS pipeline_demo.bronze.orders_raw (
        event_id STRING, order_id STRING, customer_id STRING, product_id STRING,
        quantity STRING, unit_price STRING, status STRING, region STRING,
        event_ts STRING, ingestion_ts TIMESTAMP
    ) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS pipeline_demo.silver.orders (
        order_id STRING NOT NULL, customer_id STRING NOT NULL, product_id STRING NOT NULL,
        quantity INT NOT NULL, unit_price DECIMAL(10,2) NOT NULL, amount DECIMAL(10,2) NOT NULL,
        status STRING NOT NULL, region STRING NOT NULL,
        order_date DATE NOT NULL, ingested_at TIMESTAMP NOT NULL
    ) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS pipeline_demo.gold.revenue_daily (
        order_date DATE, product_id STRING, region STRING,
        total_orders BIGINT, total_revenue DECIMAL(14,2), avg_order_value DECIMAL(10,2)
    ) USING DELTA
""")

# COMMAND ----------

# 4. Bronze ingestion — append with event-level dedup
raw_df = spark.createDataFrame(RAW_EVENTS, BRONZE_SCHEMA)

bronze_df = raw_df \
    .dropDuplicates(["event_id"]) \
    .withColumn("ingestion_ts", current_timestamp())

bronze_df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("pipeline_demo.bronze.orders_raw")

print(f"Bronze rows written: {bronze_df.count()}")  # 5 (evt-003 dropped as dup of evt-001)

# COMMAND ----------

# 5. Silver transformation — type casting and validation
bronze_raw = spark.table("pipeline_demo.bronze.orders_raw")

# Deduplicate by order_id keeping the latest event (handles status updates)
window_latest = Window.partitionBy("order_id").orderBy(desc("event_ts"))

silver_df = bronze_raw \
    .withColumn("row_num", row_number().over(window_latest)) \
    .filter(col("row_num") == 1) \
    .drop("row_num", "event_id", "ingestion_ts") \
    .withColumn("quantity",   col("quantity").cast(IntegerType())) \
    .withColumn("unit_price", col("unit_price").cast(DecimalType(10, 2))) \
    .withColumn("amount",     (col("quantity") * col("unit_price")).cast(DecimalType(10, 2))) \
    .withColumn("order_date", to_date(col("event_ts").cast(TimestampType()))) \
    .withColumn("ingested_at", current_timestamp()) \
    .filter(col("unit_price").isNotNull() & col("quantity").isNotNull()) \
    .select("order_id", "customer_id", "product_id", "quantity", "unit_price",
            "amount", "status", "region", "order_date", "ingested_at")

# COMMAND ----------

# 6. Silver MERGE — upsert on order_id (handles updates: pending→shipped, etc.)
silver_table = DeltaTable.forName(spark, "pipeline_demo.silver.orders")

silver_table.alias("target").merge(
    silver_df.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdate(set={
    "status":      "source.status",
    "ingested_at": "source.ingested_at"
}).whenNotMatchedInsertAll().execute()

print(f"Silver row count: {spark.table('pipeline_demo.silver.orders').count()}")  # 5

# COMMAND ----------

# 7. Gold aggregation — daily revenue by product and region
silver_clean = spark.table("pipeline_demo.silver.orders") \
    .filter(col("status").isin("completed", "shipped"))

gold_df = silver_clean \
    .groupBy("order_date", "product_id", "region") \
    .agg(
        count("*").alias("total_orders"),
        sum("amount").cast(DecimalType(14, 2)).alias("total_revenue"),
        avg("amount").cast(DecimalType(10, 2)).alias("avg_order_value")
    )

gold_table = DeltaTable.forName(spark, "pipeline_demo.gold.revenue_daily")

gold_table.alias("target").merge(
    gold_df.alias("source"),
    "target.order_date = source.order_date AND target.product_id = source.product_id AND target.region = source.region"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

print(f"Gold row count: {spark.table('pipeline_demo.gold.revenue_daily').count()}")

# COMMAND ----------

# 8. Validation queries
print("=== Layer Row Counts ===")
spark.sql("""
    SELECT 'bronze' as layer, COUNT(*) as cnt FROM pipeline_demo.bronze.orders_raw
    UNION ALL SELECT 'silver', COUNT(*) FROM pipeline_demo.silver.orders
    UNION ALL SELECT 'gold',   COUNT(*) FROM pipeline_demo.gold.revenue_daily
""").display()

print("\n=== Null check (silver.amount) — expect 0 ===")
spark.sql("SELECT COUNT(*) as null_amounts FROM pipeline_demo.silver.orders WHERE amount IS NULL").display()

print("\n=== ord-1002 status — expect 'shipped' ===")
spark.sql("SELECT order_id, status FROM pipeline_demo.silver.orders WHERE order_id = 'ord-1002'").display()

# COMMAND ----------

# 9. Time travel — bronze at version 0
v0 = spark.read.format("delta").option("versionAsOf", 0).table("pipeline_demo.bronze.orders_raw")
print(f"Bronze version 0 row count: {v0.count()}")

# COMMAND ----------

# 10. Optimize silver with Z-Order on common filter/join columns
spark.sql("OPTIMIZE pipeline_demo.silver.orders ZORDER BY (order_date, region)")

# COMMAND ----------

# 11. Inspect full transaction history
spark.sql("DESCRIBE HISTORY pipeline_demo.silver.orders").display()
# Operations: WRITE (from initial CREATE), MERGE (from silver upsert), OPTIMIZE

# COMMAND ----------

# 12. Set 30-day log retention on silver
spark.sql("""
    ALTER TABLE pipeline_demo.silver.orders
    SET TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 30 days')
""")
print("Retention policy set: 30 days")
