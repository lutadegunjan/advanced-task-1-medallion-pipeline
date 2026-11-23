# Advanced Task 1 – Batch and Real-Time Data Pipelines

## Project Overview

This project implements a data engineering architecture combining batch and real-time streaming pipelines using Apache Airflow, dbt, Apache NiFi, and ClickHouse.

The architecture follows the Medallion pattern: Bronze, Silver, and Gold.

## Architecture

### Batch Pipeline

CSV Dataset → Apache Airflow → ClickHouse Bronze → dbt → Silver → Gold

- Source: DHAP-34 email thread dataset
- Airflow orchestrates ingestion and dbt transformations.
- Bronze stores the raw email records.
- Silver normalizes email fields and parses timestamps.
- Gold aggregates email activity by thread.

### Streaming Pipeline

Coinbase WebSocket → Apache NiFi → ClickHouse Bronze

- Source: Coinbase Advanced Trade WebSocket
- Product: BTC-USD
- Channel: ticker
- NiFi handles WebSocket ingestion, JSON transformation, and HTTP delivery.
- ClickHouse stores streaming records in the Bronze layer.

## Technology Stack

- Python
- Apache Airflow
- Apache NiFi
- dbt
- ClickHouse
- Docker
- PowerShell

## Verified Results

| Pipeline / Layer | Records |
|---|---:|
| Email Batch – Bronze | 21,684 |
| Email Batch – Silver | 21,684 |
| Email Batch – Gold | 4,167 |
| Coinbase Streaming – Bronze | 85,335 |

These are observed counts from the verification performed on November 23, 2026. Streaming counts may increase as new events arrive.

## Repository Structure

```text
Advanced_Task_1/
├── airflow/
│   └── dags/
│       └── dhap34_email_pipeline.py
├── dbt/
│   ├── models/
│   │   ├── silver_email_threads.sql
│   │   └── gold_email_thread_summary.sql
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── dbt_project.yml
├── nifi/
│   └── coinbase_streaming_flow_documentation.txt
├── clickhouse/
│   └── table_definitions.sql
├── docs/
└── screenshots/
