from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import random
import string

DB_CONFIG = {
    "host": "postgres",
    "database": "pipeline_db",
    "user": "pipeline_user",
    "password": "pipeline_pass"
}

CATEGORIES = ["Electronics", "Clothing", "Food", "Books", "Sports", "Home", "Beauty"]
REGIONS = ["North", "South", "East", "West", "Central"]

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def generate_order_id():
    return "ORD-" + "".join(random.choices(string.digits, k=6))

def generate_sales_data(**context):
    """Placeholder for manual/UI data ingestion. No auto-rows added."""
    print("Skipping auto-generation. Expecting data to be ingested via Frontend API.")


def validate_bad_values(**context):
    """Checks for negative/zero quantity or price."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM raw_sales
        WHERE quantity <= 0 OR price <= 0
    """)
    bad = cur.fetchone()[0]
    cur.close()
    conn.close()
    if bad > 0:
        raise ValueError(f"BAD_VALUES: {bad} rows with invalid quantity or price.")
    print("Bad values check passed.")


def validate_duplicates(**context):
    """Checks for duplicate order_ids."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT order_id FROM raw_sales
            WHERE order_date = CURRENT_DATE
            GROUP BY order_id HAVING COUNT(*) > 1
        ) dupes
    """)
    dupes = cur.fetchone()[0]
    cur.close()
    conn.close()
    if dupes > 0:
        raise ValueError(f"DUPLICATES: {dupes} duplicate order_ids found.")
    print("Duplicate check passed.")


def validate_volume(**context):
    """Checks for volume anomalies — too few or too many rows."""
    conn = get_conn()
    cur = conn.cursor()
    # Get average daily row count from last 7 days
    cur.execute("""
        SELECT AVG(daily_count) FROM (
            SELECT order_date, COUNT(*) as daily_count
            FROM raw_sales
            WHERE order_date < CURRENT_DATE
            GROUP BY order_date
            ORDER BY order_date DESC
            LIMIT 7
        ) recent
    """)
    avg = cur.fetchone()[0]

    # Get today's count
    cur.execute("""
        SELECT COUNT(*) FROM raw_sales WHERE order_date = CURRENT_DATE
    """)
    today = cur.fetchone()[0]
    cur.close()
    conn.close()

    if avg and today > 0:
        ratio = today / float(avg)
        if ratio > 3.0:
            raise ValueError(f"VOLUME_ANOMALY: Today has {today} rows, {ratio:.1f}x the average {avg:.0f}.")
        if ratio < 0.3:
            raise ValueError(f"VOLUME_ANOMALY: Today has only {today} rows, {ratio:.1f}x the average {avg:.0f}.")
    print(f"Volume check passed. Today: {today} rows.")


def validate_schema(**context):
    """Checks all required columns exist in raw_sales."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'raw_sales'
    """)
    cols = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()

    required = ["order_id", "product_id", "customer_id", "quantity", "price", "category", "region", "order_date"]
    missing = [c for c in required if c not in cols]
    if missing:
        raise ValueError(f"SCHEMA_DRIFT: Missing columns: {missing}")
    print("Schema check passed.")


def summarize_data(**context):
    """Prints daily sales summary to logs."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT category, region,
               COUNT(*) as orders,
               ROUND(SUM(price * quantity)::numeric, 2) as revenue
        FROM raw_sales
        WHERE order_date = CURRENT_DATE
        GROUP BY category, region
        ORDER BY revenue DESC
        LIMIT 10
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    print("Top 10 category-region combos today:")
    for r in rows:
        print(f"  {r[0]} / {r[1]}: {r[2]} orders, ${r[3]} revenue")


default_args = {
    "owner": "abhishek",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="data_ingestion_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:

    ingest = PythonOperator(
        task_id="ingest_data",
        python_callable=generate_sales_data
    )
    check_bad_values = PythonOperator(
        task_id="validate_bad_values",
        python_callable=validate_bad_values
    )
    check_duplicates = PythonOperator(
        task_id="validate_duplicates",
        python_callable=validate_duplicates
    )
    check_volume = PythonOperator(
        task_id="validate_volume",
        python_callable=validate_volume
    )
    check_schema = PythonOperator(
        task_id="validate_schema",
        python_callable=validate_schema
    )
    summarize = PythonOperator(
        task_id="summarize_data",
        python_callable=summarize_data
    )

    ingest >> [check_bad_values, check_duplicates, check_volume, check_schema] >> summarize