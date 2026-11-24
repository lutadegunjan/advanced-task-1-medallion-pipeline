from datetime import datetime
import yaml
import pandas as pd
import clickhouse_connect

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

CSV_PATH = "/opt/airflow/data/email_thread_details.csv"
PROFILE_PATH = "/home/airflow/.dbt/profiles.yml"


def load_email_threads():
    df = pd.read_csv(CSV_PATH, dtype=str, keep_default_na=False)

    expected = ["thread_id", "subject", "timestamp", "from", "to", "body"]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"Missing CSV columns: {missing}")

    if len(df) != 21684:
        raise ValueError(f"CSV validation failed: expected 21684 rows, found {len(df)}")

    df = df[expected].rename(columns={
        "timestamp": "raw_timestamp",
        "from": "sender",
        "to": "recipient"
    })

    with open(PROFILE_PATH, "r") as profile_file:
        profile = yaml.safe_load(profile_file)["email_threads"]["outputs"]["dev"]

    client = clickhouse_connect.get_client(
        host=profile["host"],
        port=int(profile["port"]),
        username=profile["user"],
        password=profile["password"],
        database=profile["database"],
        connect_timeout=30,
        send_receive_timeout=300
    )

    try:
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

        client.command("TRUNCATE TABLE bronze.email_thread_details")
        print(f"Validated {len(df)} source rows. Starting Bronze load.", flush=True)

        batch_size = 100
        for start in range(0, len(df), batch_size):
            chunk = df.iloc[start:start + batch_size]
            client.insert(
                "email_thread_details",
                chunk[columns].values.tolist(),
                column_names=columns
            )
            print(
                f"Loaded {min(start + batch_size, len(df))}/{len(df)} rows",
                flush=True
            )

        loaded = client.query(
            "SELECT count() FROM bronze.email_thread_details"
        ).result_rows[0][0]

        if loaded != len(df):
            raise ValueError(
                f"Expected {len(df)} rows, loaded {loaded}"
            )

        print(f"SUCCESS: Loaded and verified {loaded} email records into Bronze")

    finally:
        client.close()

with DAG(
    dag_id="dhap34_email_batch_pipeline",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    tags=["DHAP-34", "ClickHouse", "Bronze", "Silver", "Gold"]
) as dag:

    load_bronze = PythonOperator(
        task_id="load_email_threads_to_bronze",
        python_callable=load_email_threads
    )

    transform_email = BashOperator(
        task_id="transform_email_threads_with_dbt",
        bash_command=(
            "/home/airflow/.local/bin/dbt run "
            "--select +gold_email_thread_summary "
            "--project-dir /opt/airflow/dbt/email_threads "
            "--profiles-dir /home/airflow/.dbt"
        )
    )

    load_bronze >> transform_email
