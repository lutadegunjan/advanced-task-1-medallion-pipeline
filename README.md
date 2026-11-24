# Advanced Task 1 – Batch and Real-Time Data Pipelines

## Project Overview

This project implements batch and real-time data pipelines using Apache Airflow, Apache NiFi, dbt, and ClickHouse.

Both pipelines follow the ClickHouse Medallion Architecture:

- **Bronze:** Raw ingested data
- **Silver:** Cleaned and transformed data
- **Gold:** Aggregated analytical data

## 1. Batch Pipeline – DHAP-34 Email Thread Dataset

**Architecture:**

DHAP-34 CSV → Apache Airflow → ClickHouse Bronze → dbt → Silver → Gold

- Source: DHAP-34 email thread dataset (`email_thread_details`).
- The dataset is associated with the DHAP-34/42 work.
- Airflow orchestrates CSV ingestion and dbt transformations.
- Bronze stores ingested email records.
- Silver normalizes sender and recipient fields and parses timestamps.
- Gold aggregates email activity by thread.

**Airflow DAG:** `dhap34_email_batch_pipeline`

The current Advanced Task 1 DAG reads the CSV dataset directly.

## 2. Real-Time Pipeline – Coinbase BTC-USD

**Architecture:**

Coinbase Advanced Trade WebSocket → Apache NiFi → ClickHouse Bronze → dbt → Silver → Gold

- Source: Coinbase Advanced Trade WebSocket.
- Endpoint: `wss://advanced-trade-ws.coinbase.com`
- Product: `BTC-USD`
- Channel: `ticker`
- NiFi manages WebSocket ingestion, JSON transformation, and HTTP delivery.
- Silver parses timestamps and converts numeric fields.
- Gold generates product-level ticker statistics.

NiFi continuously ingests into Bronze. Silver and Gold are materialized dbt tables refreshed when dbt models run.

## 3. Technology Stack

- Python
- Apache Airflow
- Apache NiFi
- dbt
- ClickHouse
- Docker
- PostgreSQL

## 4. Verified Results

Counts verified directly from ClickHouse on November 25, 2026.

| Pipeline | Layer | ClickHouse Table | Records |
|---|---|---|---:|
| Email Batch | Bronze | `bronze.email_thread_details` | 21,684 |
| Email Batch | Silver | `silver.silver_email_threads` | 21,684 |
| Email Batch | Gold | `gold.gold_email_thread_summary` | 4,167 |
| Coinbase | Bronze | `bronze.coinbase_ticker` | 362,171 |
| Coinbase | Silver | `silver.coinbase_ticker` | 361,466 |
| Coinbase | Gold | `gold.coinbase_ticker_summary` | 361,466 |

### Execution Verification

- Airflow DAG `dhap34_email_batch_pipeline`: latest run successful.
- Airflow batch ingestion and dbt transformation tasks: successful.
- Coinbase dbt models: 2 passed, 0 warnings, 0 errors.
- All six ClickHouse tables verified as populated.
- Coinbase streaming ingestion confirmed through NiFi and ClickHouse. During a 30-second live test, Bronze increased from 358,104 to 358,280 records.
- Coinbase Gold summary verified for product `BTC-USD`, with 361,466 ticker records.

These are point-in-time counts. Coinbase Bronze continues to grow as new events arrive. Silver and Gold are materialized dbt tables and reflect the most recent successful dbt refresh. Bronze can therefore contain newer events than Silver and Gold.

## 5. Repository Structure

```text
Advanced_Task_1/
├── airflow/
│   └── dags/
│       └── dhap34_email_pipeline.py
├── dbt/
│   ├── models/
│   │   ├── silver_email_threads.sql
│   │   ├── gold_email_thread_summary.sql
│   │   ├── coinbase_ticker.sql
│   │   └── coinbase_ticker_summary.sql
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── dbt_project.yml
├── nifi/
│   └── coinbase_streaming_flow_documentation.txt
├── clickhouse/
│   └── table_definitions.sql
├── docs/
├── screenshots/
├── .gitignore
└── README.md
```

## 6. Running the Pipelines

### Trigger the Batch DAG

```powershell
docker exec data_engineering_mini_project-airflow-1 airflow dags trigger dhap34_email_batch_pipeline
```

### Run Coinbase dbt Transformations

```powershell
docker exec data_engineering_mini_project-airflow-1 bash -c "dbt run --select +coinbase_ticker_summary --project-dir /opt/airflow/dbt/email_threads --profiles-dir /home/airflow/.dbt"
```

NiFi handles Coinbase WebSocket ingestion independently.

## 7. Security

Credentials and local environment configuration are excluded from version control.

Do not commit `.env` files, dbt profiles, passwords, or access tokens.

## 8. Demo Video

Demo video link: Pending recording and upload.
