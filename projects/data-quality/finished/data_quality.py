# Databricks notebook source
# Data Quality Framework — Finished Solution

# COMMAND ----------

from dataclasses import dataclass
from typing import List
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
import random
from datetime import date, timedelta

# COMMAND ----------

# 1. DataQualityCheck definition
@dataclass
class DataQualityCheck:
    name: str
    expression: str
    severity: str = "ERROR"
    description: str = ""

ORDER_CHECKS = [
    DataQualityCheck("amount_not_null",      "amount IS NOT NULL",                                  "ERROR",   "Order amount cannot be null"),
    DataQualityCheck("amount_positive",      "amount > 0",                                           "ERROR",   "Order amount must be positive"),
    DataQualityCheck("valid_status",         "status IN ('pending', 'completed', 'shipped', 'cancelled')", "ERROR", "Status must be a known value"),
    DataQualityCheck("order_date_not_future","order_date <= current_date()",                         "WARNING", "Order date should not be in the future"),
    DataQualityCheck("customer_id_not_null", "customer_id IS NOT NULL",                              "ERROR",   "Customer ID cannot be null"),
]

# COMMAND ----------

# 2. Create infrastructure (idempotent)
spark.sql("CREATE CATALOG IF NOT EXISTS pipeline_demo")
spark.sql("CREATE SCHEMA IF NOT EXISTS pipeline_demo.monitoring")
spark.sql("CREATE SCHEMA IF NOT EXISTS pipeline_demo.silver")

spark.sql("""
    CREATE TABLE IF NOT EXISTS pipeline_demo.monitoring.dq_results (
        run_timestamp TIMESTAMP, table_name STRING, check_name STRING, severity STRING,
        total_rows BIGINT, pass_count BIGINT, fail_count BIGINT, fail_pct DOUBLE
    ) USING DELTA
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS pipeline_demo.monitoring.quarantine (
        order_id STRING, customer_id STRING, amount DOUBLE, status STRING,
        order_date DATE, fail_reason STRING, quarantine_ts TIMESTAMP
    ) USING DELTA
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS pipeline_demo.silver.orders_dq (
        order_id STRING, customer_id STRING, amount DOUBLE, status STRING, order_date DATE
    ) USING DELTA
""")

# COMMAND ----------

# 3. DataQualityRunner — the core framework class
class DataQualityRunner:
    def __init__(self, checks: List[DataQualityCheck], table_name: str):
        self.checks = checks
        self.table_name = table_name

    def evaluate(self, df: DataFrame):
        # Add a boolean column per check
        df_flagged = df
        for check in self.checks:
            df_flagged = df_flagged.withColumn(check.name, expr(check.expression))

        # A row fails if ANY error-severity check is False
        error_checks = [c for c in self.checks if c.severity == "ERROR"]
        fail_condition = reduce(
            lambda a, b: a | b,
            [~col(c.name) for c in error_checks]
        )

        passing_df = df_flagged.filter(~fail_condition)
        failing_df = df_flagged.filter(fail_condition)

        self._df_flagged = df_flagged  # keep for monitoring
        return passing_df, failing_df

    def _build_fail_reason(self, failing_df: DataFrame) -> DataFrame:
        reason_col = array_join(
            array_remove(
                array(*[when(~col(c.name), lit(c.name)) for c in self.checks]),
                None
            ),
            ", "
        )
        return failing_df.withColumn("fail_reason", reason_col)

    def _drop_check_cols(self, df: DataFrame) -> DataFrame:
        return df.drop(*[c.name for c in self.checks])

    def write_results(self, passing_df: DataFrame, failing_df: DataFrame) -> None:
        check_cols = [c.name for c in self.checks]

        # Write clean rows to target table
        self._drop_check_cols(passing_df) \
            .write.format("delta").mode("append") \
            .saveAsTable(self.table_name)

        # Write failing ERROR rows to quarantine with timestamp
        error_failing = self._drop_check_cols(failing_df) \
            .withColumn("quarantine_ts", current_timestamp())

        error_failing.write.format("delta").mode("append") \
            .saveAsTable("pipeline_demo.monitoring.quarantine")

        print(f"PASSED: {passing_df.count()} rows → {self.table_name}")
        print(f"QUARANTINED: {failing_df.count()} rows → pipeline_demo.monitoring.quarantine")

    def log_to_monitoring(self) -> list:
        df = self._df_flagged
        total = df.count()
        results = []
        for check in self.checks:
            passed = df.filter(col(check.name)).count()
            failed = total - passed
            fail_pct = round(failed / total * 100, 2) if total > 0 else 0.0
            results.append({
                "run_timestamp": None,  # set below
                "table_name":    self.table_name,
                "check_name":    check.name,
                "severity":      check.severity,
                "total_rows":    total,
                "pass_count":    passed,
                "fail_count":    failed,
                "fail_pct":      fail_pct,
            })

        MONITORING_SCHEMA = StructType([
            StructField("run_timestamp", TimestampType(), True),
            StructField("table_name",    StringType(),    True),
            StructField("check_name",    StringType(),    True),
            StructField("severity",      StringType(),    True),
            StructField("total_rows",    LongType(),      True),
            StructField("pass_count",    LongType(),      True),
            StructField("fail_count",    LongType(),      True),
            StructField("fail_pct",      DoubleType(),    True),
        ])

        # Build rows with current timestamp
        rows = [(current_timestamp(), r["table_name"], r["check_name"], r["severity"],
                 r["total_rows"], r["pass_count"], r["fail_count"], r["fail_pct"])
                for r in results]

        spark.createDataFrame(results) \
            .withColumn("run_timestamp", current_timestamp()) \
            .write.format("delta").mode("append") \
            .saveAsTable("pipeline_demo.monitoring.dq_results")

        return results

    def quality_score(self, run_summary: list) -> float:
        weight_map = {"ERROR": 2, "WARNING": 1}
        sev_by_name = {c.name: c.severity for c in self.checks}
        weighted_pass = sum(weight_map[sev_by_name[r["check_name"]]] * r["pass_count"] for r in run_summary)
        weighted_total = sum(weight_map[sev_by_name[r["check_name"]]] * r["total_rows"] for r in run_summary)
        return round(weighted_pass / weighted_total * 100, 1) if weighted_total > 0 else 0.0

# COMMAND ----------

# 4. Generate sample data with intentional quality issues
random.seed(99)

ORDERS_SCHEMA = StructType([
    StructField("order_id",    StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount",      DoubleType(), True),
    StructField("status",      StringType(), True),
    StructField("order_date",  DateType(),   True),
])

def make_orders(n: int, bad_pct: float = 0.10):
    statuses = ["completed", "shipped", "pending", "cancelled"]
    rows = []
    for i in range(n):
        is_bad = random.random() < bad_pct
        amount = None if (is_bad and random.random() < 0.4) else round(random.uniform(10, 500), 2)
        status = "INVALID_STATUS" if (is_bad and random.random() < 0.3) else random.choice(statuses)
        days_offset = 30 if (is_bad and random.random() < 0.3) else -random.randint(0, 90)
        order_date = date.today() + timedelta(days=days_offset)
        customer_id = None if (is_bad and random.random() < 0.2) else f"cust-{random.randint(1, 50):04d}"
        rows.append((f"ord-{i:06d}", customer_id, amount, status, order_date))
    return rows

orders_raw = spark.createDataFrame(make_orders(200, bad_pct=0.10), ORDERS_SCHEMA)
print(f"Total orders: {orders_raw.count()}")

# COMMAND ----------

# 5. Run the framework
runner = DataQualityRunner(ORDER_CHECKS, "pipeline_demo.silver.orders_dq")

passing_df, failing_df = runner.evaluate(orders_raw)
failing_with_reason = runner._build_fail_reason(failing_df)

runner.write_results(passing_df, failing_with_reason)
run_summary = runner.log_to_monitoring()

score = runner.quality_score(run_summary)
print(f"\nQuality score: {score}%")

# COMMAND ----------

# 6. Inspect quarantine table
print("\n=== Quarantine Sample ===")
spark.table("pipeline_demo.monitoring.quarantine").display()

# COMMAND ----------

# 7. Monitoring table
print("\n=== DQ Results ===")
spark.table("pipeline_demo.monitoring.dq_results") \
    .select("check_name", "severity", "total_rows", "pass_count", "fail_count", "fail_pct") \
    .display()

# COMMAND ----------

# 8. Quality trend — run at different bad rates and observe trend
for bad_pct in [0.05, 0.20, 0.35]:
    orders_test = spark.createDataFrame(make_orders(200, bad_pct=bad_pct), ORDERS_SCHEMA)
    r = DataQualityRunner(ORDER_CHECKS, "pipeline_demo.silver.orders_dq")
    p, f = r.evaluate(orders_test)
    r._build_fail_reason(f)
    r.write_results(p, r._build_fail_reason(f))
    summary = r.log_to_monitoring()
    print(f"bad_pct={bad_pct:.0%} → quality score: {r.quality_score(summary)}%")

# COMMAND ----------

# 9. Trend analysis query
spark.sql("""
    SELECT check_name, run_timestamp, fail_pct
    FROM pipeline_demo.monitoring.dq_results
    ORDER BY check_name, run_timestamp
""").display()
