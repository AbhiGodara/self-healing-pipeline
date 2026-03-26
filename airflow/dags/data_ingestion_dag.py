from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import random
import os

DB_CONFIG = {
    "host": "postgres",
    "database": "pipeline_db",
    "user": "pipeline_user",
    "password": "pipeline_pass"
}

def generate_sales_data(**context):
    """Generates fake sales rows and inserts into Postgres."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for _ in range(50):
        cur.execute("""
            INSERT INTO raw_sales (product_id, quantity, price, sale_date)
            VALUES (%s, %s, %s, %s)
        """, (
            f"PROD-{random.randint(1, 20):03d}",
            random.randint(1, 100),
            round(random.uniform(5.0, 500.0), 2),
            datetime.now().date()
        ))
    conn.commit()
    cur.close()
    conn.close()
    print("Inserted 50 sales rows successfully.")

def validate_data(**context):
    """Checks that inserted rows have no nulls or negative values."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM raw_sales
        WHERE quantity <= 0 OR price <= 0 OR product_id IS NULL
    """)
    bad_rows = cur.fetchone()[0]
    cur.close()
    conn.close()
    if bad_rows > 0:
        raise ValueError(f"Data validation failed: {bad_rows} bad rows found.")
    print("Validation passed.")

def summarize_data(**context):
    """Aggregates daily sales summary — prints to logs."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT product_id, SUM(quantity), ROUND(SUM(price*quantity)::numeric, 2)
        FROM raw_sales
        WHERE sale_date = CURRENT_DATE
        GROUP BY product_id
        ORDER BY 3 DESC
        LIMIT 5
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    print("Top 5 products today:")
    for r in rows:
        print(f"  {r[0]}: qty={r[1]}, revenue={r[2]}")

default_args = {
    "owner": "abhishek",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="data_ingestion_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/5 * * * *",  # every 5 minutes
    catchup=False,
) as dag:

    ingest = PythonOperator(task_id="ingest_data", python_callable=generate_sales_data)
    validate = PythonOperator(task_id="validate_data", python_callable=validate_data)
    summarize = PythonOperator(task_id="summarize_data", python_callable=summarize_data)

    ingest >> validate >> summarize