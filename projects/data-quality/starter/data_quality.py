# Databricks notebook source
# Data Quality Framework — Starter
# Build a reusable quality check system with quarantine and monitoring

# COMMAND ----------

from dataclasses import dataclass
from typing import List, Optional
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
import random
from datetime import date, timedelta

# COMMAND ----------

# 1. DataQualityCheck dataclass — pre-defined, no changes needed
@dataclass
class DataQualityCheck:
    name: str
    expression: str       # SQL boolean expression — True = PASS
    severity: str = "ERROR"  # "ERROR" = quarantine failing rows; "WARNING" = log only
    description: str = ""

# Pre-defined checks for an orders table
ORDER_CHECKS = [
    DataQualityCheck(
        name="amount_not_null",
        expression="amount IS NOT NULL",
        severity="ERROR",
        description="Order amount cannot be null"
    ),
    DataQualityCheck(
        name="amount_positive",
        expression="amount > 0",
        severity="ERROR",
        description="Order amount must be positive"
    ),
    DataQualityCheck(
        name="valid_status",
        expression="status IN ('pending', 'completed', 'shipped', 'cancelled')",
        severity="ERROR",
        description="Status must be a known value"
    ),
    DataQualityCheck(
        name="order_date_not_future",
        expression="order_date <= current_date()",
        severity="WARNING",
        description="Order date should not be in the future"
    ),
    DataQualityCheck(
        name="customer_id_not_null",
        expression="customer_id IS NOT NULL",
        severity="ERROR",
        description="Customer ID cannot be null"
    ),
]

# COMMAND ----------

# 2. Create monitoring table (run once)
spark.sql("CREATE CATALOG IF NOT EXISTS pipeline_demo")
spark.sql("CREATE SCHEMA IF NOT EXISTS pipeline_demo.monitoring")

spark.sql("""
    CREATE TABLE IF NOT EXISTS pipeline_demo.monitoring.dq_results (
        run_timestamp   TIMESTAMP,
        table_name      STRING,
        check_name      STRING,
        severity        STRING,
        total_rows      BIGINT,
        pass_count      BIGINT,
        fail_count      BIGINT,
        fail_pct        DOUBLE
    ) USING DELTA
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS pipeline_demo.monitoring.quarantine (
        order_id    STRING,
        customer_id STRING,
        amount      DOUBLE,
        status      STRING,
        order_date  DATE,
        fail_reason STRING,
        quarantine_ts TIMESTAMP
    ) USING DELTA
""")

# COMMAND ----------

# 3. Generate sample orders with intentional quality issues
random.seed(99)

def make_orders(n: int, bad_pct: float = 0.10):
    rows = []
    statuses = ["completed", "shipped", "pending", "cancelled"]
    for i in range(n):
        is_bad = random.random() < bad_pct
        amount = None if (is_bad and random.random() < 0.4) else round(random.uniform(10, 500), 2)
        status = "INVALID_STATUS" if (is_bad and random.random() < 0.3) else random.choice(statuses)
        days_offset = 30 if (is_bad and random.random() < 0.3) else -random.randint(0, 90)
        order_date = date.today() + timedelta(days=days_offset)
        customer_id = None if (is_bad and random.random() < 0.2) else f"cust-{random.randint(1, 50):04d}"
        rows.append((f"ord-{i:06d}", customer_id, amount, status, order_date))
    return rows

ORDERS_SCHEMA = StructType([
    StructField("order_id",    StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount",      DoubleType(), True),
    StructField("status",      StringType(), True),
    StructField("order_date",  DateType(),   True),
])

orders_raw = spark.createDataFrame(make_orders(200, bad_pct=0.10), ORDERS_SCHEMA)
print(f"Total orders: {orders_raw.count()}")

# COMMAND ----------

# --- TODO 1: Implement DataQualityRunner class ---
# class DataQualityRunner:
#   def __init__(self, checks: List[DataQualityCheck], table_name: str, spark_session=None):
#       self.checks = checks
#       self.table_name = table_name
#       self.spark = spark_session or spark
#
# Hint: store checks as instance variable, table_name for logging

raise NotImplementedError("TODO 1: Define DataQualityRunner.__init__")

# COMMAND ----------

# --- TODO 2: Implement evaluate() method ---
# evaluate(df: DataFrame) -> tuple[DataFrame, DataFrame]
# Returns (passing_df, failing_df)
#
# Steps:
#   a. For each check, add a boolean column: df.withColumn(check.name, expr(check.expression))
#   b. A row PASSES if ALL check columns are True
#   c. A row FAILS if ANY check column is False (for ERROR severity checks)
#
# Hint: build a fail_condition using reduce():
#   from functools import reduce
#   error_checks = [c for c in self.checks if c.severity == "ERROR"]
#   fail_condition = reduce(lambda a, b: a | b,
#                           [~col(c.name) for c in error_checks])
#
# Hint: passing_df  = df_with_flags.filter(~fail_condition)
#       failing_df  = df_with_flags.filter(fail_condition)

raise NotImplementedError("TODO 2: Implement evaluate() to split passing/failing rows")

# COMMAND ----------

# --- TODO 3: Implement _build_fail_reason() helper ---
# Given a DataFrame with check flag columns, add a fail_reason STRING column
# that concatenates the names of all failed checks.
#
# Hint: use array_join + array_remove + array():
#   reason_col = array_join(
#       array_remove(
#           array(*[when(~col(c.name), lit(c.name)) for c in self.checks]),
#           None
#       ),
#       ", "
#   )
#   return df.withColumn("fail_reason", reason_col)

raise NotImplementedError("TODO 3: Implement _build_fail_reason()")

# COMMAND ----------

# --- TODO 4: Implement write_results() method ---
# write_results(passing_df: DataFrame, failing_df: DataFrame) -> None
#
# - Write passing rows to self.table_name (append mode)
#   Hint: passing_df.drop all check flag columns before writing
#         .write.format("delta").mode("append").saveAsTable(self.table_name)
#
# - Write ERROR failing rows to pipeline_demo.monitoring.quarantine
#   with a fail_reason column and quarantine_ts = current_timestamp()
#
# - Print summary: "{pass_count} rows passed, {fail_count} rows quarantined"

raise NotImplementedError("TODO 4: Implement write_results()")

# COMMAND ----------

# --- TODO 5: Implement log_to_monitoring() method ---
# log_to_monitoring(df_with_flags: DataFrame) -> None
#
# Write one row per check to pipeline_demo.monitoring.dq_results:
#   - run_timestamp: current_timestamp()
#   - table_name: self.table_name
#   - check_name, severity from the check definition
#   - total_rows, pass_count (rows where check is True), fail_count, fail_pct
#
# Hint: for each check:
#   total = df_with_flags.count()
#   passed = df_with_flags.filter(col(check.name)).count()
#   failed = total - passed
#   fail_pct = round(failed / total * 100, 2)
#
# Hint: collect results into a list of dicts, then
#   spark.createDataFrame(rows, MONITORING_SCHEMA).write...saveAsTable(...)

raise NotImplementedError("TODO 5: Implement log_to_monitoring()")

# COMMAND ----------

# --- TODO 6: Implement quality_score() method ---
# quality_score(run_summary: list[dict]) -> float
#
# Returns weighted pass rate:
#   - ERROR checks count with weight 2
#   - WARNING checks count with weight 1
#   - score = sum(weight * pass_count) / sum(weight * total_rows) * 100
#
# Hint: iterate over run_summary dicts, look up severity in self.checks by check_name

raise NotImplementedError("TODO 6: Implement quality_score()")

# COMMAND ----------

# --- TODO 7: Wire it all together ---
# 1. Instantiate DataQualityRunner(ORDER_CHECKS, "pipeline_demo.silver.orders_dq")
# 2. Call evaluate(orders_raw) → (passing_df, failing_df)
# 3. Call _build_fail_reason(failing_df) → failing_with_reason
# 4. Call write_results(passing_df, failing_with_reason)
# 5. Call log_to_monitoring(df_with_flags)
# 6. Call quality_score() and print the result
# 7. Verify: display quarantine table — do fail_reason values match which checks failed?

raise NotImplementedError("TODO 7: Run the framework on orders_raw and inspect output")

# COMMAND ----------

# --- TODO 8: Quality trend analysis ---
# Run the framework 3 more times with different bad_pct values: 0.05, 0.20, 0.35
# After each run, the monitoring table gets new rows.
# Query monitoring.dq_results to show fail_pct trend per check over time.

# Expected query:
# SELECT check_name, run_timestamp, fail_pct
# FROM pipeline_demo.monitoring.dq_results
# ORDER BY check_name, run_timestamp

raise NotImplementedError("TODO 8: Run quality checks at 5%, 20%, 35% bad rates and show trend")
