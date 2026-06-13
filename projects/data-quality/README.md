# Project: Data Quality Framework

Reusable data quality check system with quarantine, monitoring, and quality score.

## What it builds

A `DataQualityRunner` class that:
- Runs configurable SQL-expression-based checks against any DataFrame
- Routes passing rows to target table, failing rows to quarantine
- Logs per-check metrics to a monitoring Delta table
- Computes a weighted quality score per run

## Running it

Paste cells into a Databricks notebook. All infrastructure (catalog, schemas, tables) is created idempotently on first run.
Requires DBR 13.3 LTS or later.

## Files

- `starter/data_quality.py` — Class skeleton with TODO 1–8 placeholders
- `finished/data_quality.py` — Complete implementation with trend analysis
