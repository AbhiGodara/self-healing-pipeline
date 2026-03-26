import psycopg2
import sys

DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "pipeline_db",
    "user": "pipeline_user",
    "password": "pipeline_pass"
}

def inject_bad_data():
    """Inserts rows with negative quantity — will fail validation."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for _ in range(10):
        cur.execute("""
            INSERT INTO raw_sales (product_id, quantity, price, sale_date)
            VALUES ('BAD-PRODUCT', -99, -1.0, CURRENT_DATE)
        """)
    conn.commit()
    cur.close()
    conn.close()
    print("Injected 10 bad rows. Next DAG run will fail at validation.")

def corrupt_schema():
    """Drops the price column — will cause ingestion to crash."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("ALTER TABLE raw_sales DROP COLUMN IF EXISTS price;")
    conn.commit()
    cur.close()
    conn.close()
    print("Corrupted schema. Pipeline will crash on next run.")

def restore_schema():
    """Adds price column back."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("ALTER TABLE raw_sales ADD COLUMN IF NOT EXISTS price FLOAT;")
    conn.commit()
    cur.close()
    conn.close()
    print("Schema restored.")

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "bad_data"
    if mode == "bad_data":
        inject_bad_data()
    elif mode == "corrupt":
        corrupt_schema()
    elif mode == "restore":
        restore_schema()
    else:
        print("Usage: python simulate_failure.py [bad_data|corrupt|restore]")