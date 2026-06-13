# PySpark to Databricks: Deep Dive

A self-contained HTML learning guide for engineers who already know basic PySpark and want to go deeper — Delta Lake, Spark SQL mastery, performance engineering, and the Databricks platform.

## What this covers

**Part I: The Databricks World**
- Cluster architecture, DBR vs OSS Spark, spot instances, DBFS vs external storage
- Notebooks, magic commands, `dbutils`, widgets, `display()` vs `.show()`
- Delta Lake: ACID, time travel, MERGE, OPTIMIZE/Z-Order, schema evolution, CDF

**Part II: Spark SQL Mastery**
- Window functions: ROW_NUMBER, RANK, LAG/LEAD, aggregate windows, QUALIFY
- Advanced SQL: CTEs, PIVOT, arrays/structs, JSON parsing, LATERAL VIEW, EXECUTE IMMEDIATE
- SQL performance: EXPLAIN, predicate pushdown, join hints, CBO, Spark cache vs Delta cache

**Part III: Performance Engineering**
- Data layout: disk partitioning, Z-Order, Liquid Clustering (DBR 13.3+), Auto Optimize
- Join strategies: broadcast, sort-merge, skew detection, salting, AQE skew handling
- Catalyst optimizer & AQE: rule-based optimizations, dynamic partition coalescing

**Part IV: The Platform**
- Workflows: job DAGs, task types, task values, error handling, cron scheduling
- Unity Catalog: three-level namespace, GRANT/REVOKE, row filters, column masking
- AWS integration: instance profiles, Databricks secrets, S3 access patterns

**Part V: Building Things (Projects)**
- [ETL Pipeline](projects/etl-pipeline/) — Bronze→Silver→Gold medallion with MERGE, time travel, OPTIMIZE
- [SQL Analytics](projects/sql-analytics/) — Cohort retention, MoM growth, QUALIFY dedup, PIVOT, EXPLAIN tuning
- [Data Quality Framework](projects/data-quality/) — Configurable checks, quarantine table, monitoring Delta table

## Assumed knowledge

Basic PySpark: DataFrames, `filter`, `groupBy`, `join`, basic SQL queries. This guide picks up from there.

## Running it

Open `index.html` in a browser — no build step, no server needed. All CSS, JS, and content are self-contained.

For the project challenges, paste the notebook cells (separated by `# COMMAND ----------`) into a Databricks notebook running DBR 13.3 LTS or later with Unity Catalog enabled.

## Stack

- Pure HTML/CSS/JS — no framework
- [Prism.js](https://prismjs.com/) for syntax highlighting
- Dark theme optimized for readability
- Databricks brand red (`#ff3621`) + Spark amber (`#f5a623`) accent colors
