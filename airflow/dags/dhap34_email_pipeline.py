from datetime import datetime
import pandas as pd
import clickhouse_connect
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

CSV_PATH = "/opt/airflow/data/email_thread_details.csv"

def load_email_threads():
    df = pd.read_csv(CSV_PATH, dtype=str, keep_default_na=False)

    expected = ["thread_id", "subject", "timestamp", "from", "to", "body"]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"Missing CSV columns: {missing}")

    df = df[expected].rename(columns={
        "timestamp": "raw_timestamp",
        "from": "sender",
        "to": "recipient"
    })

    client = clickhouse_connect.get_client(
        host="host.docker.internal",
        port=8123,
        username="batch",
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database="bronze"
    )

    client.command("""
        CREATE TABLE IF NOT EXISTS bronze.email_thread_details (
            thread_id String,
            subject String,
            raw_timestamp String,
            sender String,
            recipient String,
            body String
        )
        ENGINE = MergeTree
        ORDER BY (thread_id, raw_timestamp)
    """)


    columns = [
        "thread_id", "subject", "raw_timestamp",
        "sender", "recipient", "body"
    ]

    for start in range(0, len(df), 1000):
        chunk = df.iloc[start:start + 1000]
        client.insert(
            "email_thread_details",
            chunk[columns].values.tolist(),
            column_names=columns
        )

    loaded = client.query(
        "SELECT count() FROM bronze.email_thread_details"
    ).result_rows[0][0]

    if loaded != len(df):
        raise ValueError(f"Expected {len(df)} rows, loaded {loaded}")

    print(f"SUCCESS: Loaded and verified {loaded} email records into Bronze")
    client.close()


with DAG(
    dag_id="dhap34_email_batch_pipeline",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    tags=["DHAP-34", "ClickHouse", "Bronze"]
) as dag:

    load_bronze = PythonOperator(
        task_id="load_email_threads_to_bronze",
        python_callable=load_email_threads
    )


