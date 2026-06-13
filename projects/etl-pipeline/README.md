# Project: ETL Pipeline

Bronze → Silver → Gold medallion ETL pipeline using Delta Lake.

## What it builds

A daily order processing pipeline that:
- Ingests raw JSON events to Bronze (append with event dedup)
- Transforms and validates to Silver (MERGE for upsert semantics)
- Aggregates to Gold (daily revenue by product and region)

## Running it

Paste the notebook cells into a Databricks notebook (cells are separated by `# COMMAND ----------`).
Requires DBR 13.3 LTS or later with Unity Catalog enabled.

## Files

- `starter/pipeline.py` — Schemas pre-defined; TODO 1–9 for you to implement
- `finished/pipeline.py` — Complete solution with numbered inline comments
