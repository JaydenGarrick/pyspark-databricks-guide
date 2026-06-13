# Project: SQL Analytics

Complex Spark SQL analytics suite — window functions, cohort analysis, QUALIFY, PIVOT, and query tuning.

## What it builds

Eight analytics queries covering:
- QUALIFY-based deduplication (top-N without subqueries)
- Month-over-month growth with LAG
- Cohort retention table
- Channel revenue attribution with running totals
- PIVOT by product category
- EXPLAIN + BROADCAST hint performance tuning

## Running it

Paste cells into a Databricks notebook. All data is generated in-memory — no external dependencies.
Requires DBR 13.3 LTS or later (for QUALIFY support).

## Files

- `starter/analytics.py` — Data generation and query skeletons with TODO placeholders
- `finished/analytics.py` — Complete solutions with inline explanations
