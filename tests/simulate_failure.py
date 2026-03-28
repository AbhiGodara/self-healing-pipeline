import sys
import subprocess

POSTGRES = "self_healing_pipeline-postgres-1"
AIRFLOW  = "self_healing_pipeline-airflow-webserver-1"

def run_sql(query: str) -> str:
    result = subprocess.run([
        "docker", "exec", POSTGRES,
        "psql", "-U", "pipeline_user", "-d", "pipeline_db", "-c", query
    ], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  ⚠ SQL error: {result.stderr.strip()}")
    return result.stdout.strip()

def trigger_dag():
    subprocess.run([
        "docker", "exec", AIRFLOW,
        "airflow", "dags", "trigger", "data_ingestion_pipeline"
    ], capture_output=True)
    print("  → DAG triggered.")

# ── FAILURE TYPE 1: Bad values ─────────────────────────────────
def inject_bad_data():
    print("Injecting 10 bad-value rows (qty=-99, price=-1.0)...")
    run_sql("""
        INSERT INTO raw_sales (order_id, product_id, customer_id, quantity, price, category, region, order_date)
        SELECT
            'BAD-ORD-' || gs,
            'BAD-PROD',
            'CUST-0000',
            -99,
            -1.0,
            'Unknown',
            'North',
            CURRENT_DATE
        FROM generate_series(1, 10) gs;
    """)
    print("✓ Done — 10 bad rows inserted.")
    trigger_dag()
    print("  Next DAG run will fail at validate_bad_values.")

# ── FAILURE TYPE 2: Duplicate order_ids ───────────────────────
def inject_duplicates():
    print("Injecting 5 duplicate order_id pairs (10 rows)...")
    run_sql("""
        INSERT INTO raw_sales (order_id, product_id, customer_id, quantity, price, category, region, order_date)
        SELECT
            'DUP-ORD-' || gs,
            'PROD-001',
            'CUST-0001',
            5,
            29.99,
            'Electronics',
            'North',
            CURRENT_DATE
        FROM generate_series(1, 5) gs;

        INSERT INTO raw_sales (order_id, product_id, customer_id, quantity, price, category, region, order_date)
        SELECT
            'DUP-ORD-' || gs,
            'PROD-001',
            'CUST-0001',
            5,
            29.99,
            'Electronics',
            'North',
            CURRENT_DATE
        FROM generate_series(1, 5) gs;
    """)
    print("✓ Done — 5 duplicate order_id pairs inserted.")
    trigger_dag()
    print("  Next DAG run will fail at validate_duplicates.")

# ── FAILURE TYPE 3: Schema drift ──────────────────────────────
def corrupt_schema():
    print("Dropping 'price' column from raw_sales...")
    run_sql("ALTER TABLE raw_sales DROP COLUMN IF EXISTS price;")
    print("✓ Done — schema corrupted.")
    trigger_dag()
    print("  Next DAG run will fail at validate_schema.")

# ── FAILURE TYPE 4: Stale data ────────────────────────────────
def inject_stale_data():
    print("Deleting all of today's rows...")
    result = run_sql("DELETE FROM raw_sales WHERE order_date = CURRENT_DATE;")
    print(f"✓ Done — {result}")
    print("  Monitor agent will detect stale_data and retrigger ingest.")

# ── FAILURE TYPE 5: Volume anomaly ────────────────────────────
def inject_volume_anomaly():
    print("Injecting 500 rows (volume anomaly)...")
    run_sql("""
        INSERT INTO raw_sales (order_id, product_id, customer_id, quantity, price, category, region, order_date)
        SELECT
            'VOL-ORD-' || gs,
            'PROD-' || LPAD((gs % 50 + 1)::text, 3, '0'),
            'CUST-' || LPAD((gs % 200 + 1)::text, 4, '0'),
            (gs % 20) + 1,
            ROUND((10.0 + (gs % 100))::numeric, 2),
            'Electronics',
            'North',
            CURRENT_DATE
        FROM generate_series(1, 500) gs;
    """)
    print("✓ Done — 500 rows inserted (~10x normal average).")
    trigger_dag()
    print("  Next DAG run will fail at validate_volume.")

# ── RESTORE HELPERS ───────────────────────────────────────────
def restore_schema():
    print("Restoring 'price' column...")
    run_sql("ALTER TABLE raw_sales ADD COLUMN IF NOT EXISTS price FLOAT;")
    print("✓ Done — schema restored.")

def clean_bad_rows():
    print("Cleaning up all injected test rows...")
    run_sql("DELETE FROM raw_sales WHERE quantity <= 0 OR price <= 0;")
    run_sql("DELETE FROM raw_sales WHERE order_id LIKE 'BAD-%' OR order_id LIKE 'DUP-%' OR order_id LIKE 'VOL-%';")
    print("✓ Done — test rows removed.")

def show_status():
    print("\n── Current DB Status ──────────────────────────────────")
    print(run_sql("SELECT COUNT(*) AS total_rows_today FROM raw_sales WHERE order_date = CURRENT_DATE;"))
    print(run_sql("SELECT COUNT(*) AS bad_value_rows FROM raw_sales WHERE quantity <= 0 OR price <= 0;"))
    print(run_sql("""
        SELECT COUNT(*) AS duplicate_order_groups FROM (
            SELECT order_id FROM raw_sales
            WHERE order_date = CURRENT_DATE
            GROUP BY order_id HAVING COUNT(*) > 1
        ) d;
    """))
    print(run_sql("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'raw_sales' ORDER BY ordinal_position;
    """))
    print("───────────────────────────────────────────────────────\n")

# ── MAIN ──────────────────────────────────────────────────────

USAGE = """
Usage: python tests/simulate_failure.py <mode>

Failure modes:
  bad_data    → 10 rows with qty=-99, price=-1.0   (triggers validate_bad_values)
  duplicates  → 5 duplicate order_id pairs          (triggers validate_duplicates)
  corrupt     → drops price column                  (triggers validate_schema)
  stale       → deletes today's rows                (triggers stale_data detection)
  volume      → 500 rows injected                   (triggers validate_volume)

Restore helpers:
  restore     → adds price column back
  clean       → removes BAD/DUP/VOL test rows
  status      → shows current DB state
"""

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "help"
    modes = {
        "bad_data":   inject_bad_data,
        "duplicates": inject_duplicates,
        "corrupt":    corrupt_schema,
        "stale":      inject_stale_data,
        "volume":     inject_volume_anomaly,
        "restore":    restore_schema,
        "clean":      clean_bad_rows,
        "status":     show_status,
    }
    if mode in modes:
        modes[mode]()
    else:
        print(USAGE)