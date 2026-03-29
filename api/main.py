import subprocess
import re
import json
import os
import sys
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncio

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from orchestrator.langgraph_graph import run_self_healing_pipeline

app = FastAPI(title="Self-Healing Pipeline API", version="1.0.0")

# Allow React frontend to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── HELPERS ───────────────────────────────────────────────────

def parse_int(text: str) -> int:
    match = re.search(r'\d+', text)
    return int(match.group()) if match else 0

def run_sql(query: str) -> str:
    result = subprocess.run([
        "docker", "exec",
        "self_healing_pipeline-postgres-1",
        "psql", "-U", "pipeline_user", "-d", "pipeline_db", "-c", query
    ], capture_output=True, text=True, timeout=30)
    return result.stdout.strip()

def run_airflow(cmd: list) -> str:
    result = subprocess.run([
        "docker", "exec",
        "self_healing_pipeline-airflow-webserver-1"
    ] + cmd, capture_output=True, text=True, timeout=30)
    return result.stdout.strip()

# ── ROUTES ───────────────────────────────────────────────────

@app.get("/")
def root():
    return {"status": "ok", "message": "Self-Healing Pipeline API"}


@app.get("/health")
def get_pipeline_health():
    """Returns current pipeline health — row counts, bad rows, schema status."""
    total = parse_int(run_sql("SELECT COUNT(*) FROM raw_sales;"))
    
    schema_result = run_sql("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'raw_sales' ORDER BY ordinal_position;
    """)
    required = ["product_id", "quantity", "price", "order_date"]
    schema_ok = all(col in schema_result for col in required)

    if schema_ok:
        bad = parse_int(run_sql("SELECT COUNT(*) FROM raw_sales WHERE quantity <= 0 OR price <= 0;"))
    else:
        bad = 0
        
    good = total - bad

    dupes = parse_int(run_sql("""
        SELECT COUNT(*) FROM (
            SELECT order_id FROM raw_sales WHERE order_date = CURRENT_DATE GROUP BY order_id HAVING COUNT(*) > 1
        ) d;
    """))
    # Get latest DAG run state
    runs_output = run_airflow(["airflow", "dags", "list-runs", "-d", "data_ingestion_pipeline"])
    latest_state = "unknown"
    for line in runs_output.split("\n"):
        if any(s in line for s in ["success", "failed", "running", "queued"]):
            parts = [p.strip() for p in line.split("|") if p.strip()]
            if len(parts) >= 3:
                latest_state = parts[2]
                break

    return {
        "status": "healthy" if bad == 0 and latest_state == "success" else "unhealthy",
        "total_rows": total,
        "good_rows": good,
        "bad_rows": bad,
        "duplicate_rows": dupes,
        "pending_duplicates": dupes > 0,
        "schema_ok": schema_ok,
        "latest_dag_run": latest_state,
        "timestamp": datetime.now().isoformat()
    }


@app.get("/incidents")
def get_incidents():
    """Returns recent failed DAG runs."""
    output = run_airflow(["airflow", "dags", "list-runs", "-d", "data_ingestion_pipeline"])
    incidents = []
    for line in output.split("\n"):
        if "failed" in line:
            parts = [p.strip() for p in line.split("|") if p.strip()]
            if len(parts) >= 4:
                incidents.append({
                    "dag_id": parts[0],
                    "run_id": parts[1],
                    "state": parts[2],
                    "execution_date": parts[3],
                })
    return {"incidents": incidents[:10], "total": len(incidents)}


@app.get("/audit")
def get_audit_log():
    """Returns the last 20 entries from the audit log."""
    result = run_sql("""
        SELECT id, agent_name, action_taken, reasoning, success, created_at
        FROM audit_log
        ORDER BY created_at DESC
        LIMIT 20;
    """)

    entries = []
    lines = result.strip().split("\n")
    for line in lines:
        if "|" in line and "agent_name" not in line and "---" not in line:
            parts = [p.strip() for p in line.split("|")]
            if len(parts) >= 6:
                entries.append({
                    "id": parts[0],
                    "agent_name": parts[1],
                    "action_taken": parts[2],
                    "reasoning": parts[3],
                    "success": parts[4] == "t",
                    "created_at": parts[5],
                })

    return {"entries": entries, "total": len(entries)}


@app.get("/rows")
def get_row_stats():
    """Returns row breakdown — good vs bad by date."""
    result = run_sql("""
        SELECT
            order_date,
            COUNT(*) as total,
            SUM(CASE WHEN quantity > 0 AND price > 0 THEN 1 ELSE 0 END) as good,
            SUM(CASE WHEN quantity <= 0 OR price <= 0 THEN 1 ELSE 0 END) as bad
        FROM raw_sales
        GROUP BY order_date
        ORDER BY order_date DESC
        LIMIT 7;
    """)

    rows = []
    for line in result.strip().split("\n"):
        if "|" in line and "order_date" not in line and "---" not in line:
            parts = [p.strip() for p in line.split("|")]
            if len(parts) >= 4:
                rows.append({
                    "date": parts[0],
                    "total": int(parts[1]) if parts[1].isdigit() else 0,
                    "good": int(parts[2]) if parts[2].isdigit() else 0,
                    "bad": int(parts[3]) if parts[3].isdigit() else 0,
                })

    return {"rows": rows}


@app.get("/dag-runs")
def get_dag_runs():
    """Returns last 10 DAG runs with states."""
    output = run_airflow(["airflow", "dags", "list-runs", "-d", "data_ingestion_pipeline"])
    runs = []
    for line in output.split("\n"):
        if any(s in line for s in ["success", "failed", "running", "queued"]):
            parts = [p.strip() for p in line.split("|") if p.strip()]
            if len(parts) >= 4:
                runs.append({
                    "dag_id": parts[0],
                    "run_id": parts[1],
                    "state": parts[2],
                    "execution_date": parts[3],
                })
    return {"runs": runs[:10]}

@app.get("/data")
def get_recent_data():
    """Returns the 15 most recent rows from raw_sales for visualization."""
    result = run_sql("""
        SELECT order_id, product_id, customer_id, quantity, price, order_date
        FROM raw_sales
        ORDER BY id DESC
        LIMIT 1000;
    """)
    rows = []
    for line in result.strip().split("\n"):
        if "|" in line and "order_id" not in line and "---" not in line:
            parts = [p.strip() for p in line.split("|")]
            if len(parts) >= 6:
                rows.append({
                    "order_id": parts[0],
                    "product_id": parts[1],
                    "customer_id": parts[2],
                    "quantity": parts[3],
                    "price": parts[4],
                    "order_date": parts[5]
                })
    return {"data": rows}


class HealRequest(BaseModel):
    dag_id: str = "data_ingestion_pipeline"

healing_status = {"running": False, "last_result": None}

@app.post("/heal")
def trigger_healing(request: HealRequest, background_tasks: BackgroundTasks):
    """Triggers the full self-healing cycle in the background."""
    if healing_status["running"]:
        return {"message": "Healing already in progress", "status": "busy"}

    def run_healing():
        healing_status["running"] = True
        try:
            result = run_self_healing_pipeline(request.dag_id)
            healing_status["last_result"] = {
                "status": result.get("status"),
                "fix_action": result.get("fix_action"),
                "bad_rows": result.get("bad_rows"),
                "good_rows": result.get("good_rows"),
                "timestamp": datetime.now().isoformat()
            }
        finally:
            healing_status["running"] = False

    background_tasks.add_task(run_healing)
    return {"message": "Self-healing started", "status": "started"}


@app.get("/heal/status")
def get_healing_status():
    """Returns current healing status."""
    return {
        "running": healing_status["running"],
        "last_result": healing_status["last_result"]
    }


@app.post("/inject/{failure_type}")
def inject_specific_failure(failure_type: str, background_tasks: BackgroundTasks, count: int = 10):
    """Injects specific failures and triggers DAG."""
    
    # Fast synchronous check: don't allow inserts if schema is broken!
    schema_result = run_sql("""SELECT column_name FROM information_schema.columns WHERE table_name = 'raw_sales';""")
    schema_ok = "price" in schema_result
    if not schema_ok and failure_type in ["bad_data", "duplicates", "volume"]:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="Cannot inject data: the pipeline schema is currently broken. Please Heal the pipeline first.")

    def inject():
        if failure_type == "bad_data":
            run_sql(f"""
                INSERT INTO raw_sales (order_id, product_id, customer_id, quantity, price, order_date)
                SELECT 'BAD-ORD-' || ROUND(RANDOM()*1000000) || '-' || gs, 'BAD-PRODUCT', 'CUST-0000', -(RANDOM()*100)::int, -(RANDOM()*50)::numeric(10,2), CURRENT_DATE
                FROM generate_series(1,{count}) gs;
            """)
        elif failure_type == "duplicates":
            # Real duplicate simulation: select actual existing rows and re-insert them
            db_size = parse_int(run_sql("SELECT COUNT(*) FROM raw_sales;"))
            if db_size == 0:
                # Generate valid seed data first if completely empty
                run_sql(f"""
                INSERT INTO raw_sales (order_id, product_id, customer_id, quantity, price, category, region, order_date)
                SELECT 'SEED-ORD-' || gs, 'PROD-SEED', 'CUST-SEED', 10, 50.0, 'Electronics', 'North', CURRENT_DATE
                FROM generate_series(1, {count}) gs;
                """)
            
            run_sql(f"""
                INSERT INTO raw_sales (order_id, product_id, customer_id, quantity, price, category, region, order_date)
                SELECT order_id, product_id, customer_id, quantity + 1, price, category, region, CURRENT_DATE
                FROM (SELECT * FROM raw_sales ORDER BY id DESC LIMIT {count}) subquery;
            """)
        elif failure_type == "corrupt":
            run_sql("ALTER TABLE raw_sales DROP COLUMN IF EXISTS price;")
        elif failure_type == "stale":
            run_sql("DELETE FROM raw_sales WHERE order_date = CURRENT_DATE;")
            # No need to trigger DAG, stale data is naturally detected over time, but we'll trigger it anyway to force the incident.
        elif failure_type == "volume":
            run_sql("""
                INSERT INTO raw_sales (order_id, product_id, customer_id, quantity, price, order_date)
                SELECT 'VOL-ORD-' || gs, 'PROD-VOL', 'CUST-VOL', 1, 10.0, CURRENT_DATE
                FROM generate_series(1, 500) gs;
            """)
        run_airflow(["airflow", "dags", "trigger", "data_ingestion_pipeline"])

    background_tasks.add_task(inject)
    return {"message": f"Injected {failure_type} and triggered DAG."}

class ManualDataRow(BaseModel):
    order_id: str
    product_id: str
    customer_id: str
    quantity: int
    price: float
    category: str = "Unknown"
    region: str = "Unknown"

@app.post("/ingest-manual")
def ingest_manual_data(rows: list[ManualDataRow], background_tasks: BackgroundTasks):
    """Ingests manual data provided by user or parsed from CSV by frontend."""
    if not rows: return {"message": "No data"}
    
    def process_data():
        for r in rows:
            run_sql(f"""
                INSERT INTO raw_sales (order_id, product_id, customer_id, quantity, price, category, region, order_date)
                VALUES ('{r.order_id}', '{r.product_id}', '{r.customer_id}', {r.quantity}, {r.price}, '{r.category}', '{r.region}', CURRENT_DATE);
            """)
        run_airflow(["airflow", "dags", "trigger", "data_ingestion_pipeline"])

    background_tasks.add_task(process_data)
class DuplicateResolution(BaseModel):
    action: str # 'keep_old' or 'keep_new'

@app.post("/resolve-duplicates")
def resolve_duplicates(req: DuplicateResolution, background_tasks: BackgroundTasks):
    """Human-in-the-loop endpoint to resolve duplicates."""
    def handle_dupes():
        if req.action == "keep_new":
            # Keep rows with MAX(id) per order_id, delete others
            run_sql("""
                DELETE FROM raw_sales WHERE id NOT IN (
                    SELECT MAX(id) FROM raw_sales WHERE order_date = CURRENT_DATE GROUP BY order_id
                ) AND order_date = CURRENT_DATE AND order_id IN (
                    SELECT order_id FROM raw_sales WHERE order_date = CURRENT_DATE GROUP BY order_id HAVING COUNT(*) > 1
                );
            """)
            action_desc = "Human resolved: Kept NEW rows, deleted OLD rows."
        else:
            # Keep rows with MIN(id) per order_id, delete others
            run_sql("""
                DELETE FROM raw_sales WHERE id NOT IN (
                    SELECT MIN(id) FROM raw_sales WHERE order_date = CURRENT_DATE GROUP BY order_id
                ) AND order_date = CURRENT_DATE AND order_id IN (
                    SELECT order_id FROM raw_sales WHERE order_date = CURRENT_DATE GROUP BY order_id HAVING COUNT(*) > 1
                );
            """)
            action_desc = "Human resolved: Kept OLD rows, deleted NEW rows."
            
        # Log this human action to the AI audit log!
        run_sql(f"""
            INSERT INTO audit_log (agent_name, action_taken, reasoning, success, created_at)
            VALUES ('human_in_the_loop', '{action_desc}', 'Resolved duplicates manually via UI popup', true, NOW());
        """)
        run_airflow(["airflow", "dags", "trigger", "data_ingestion_pipeline"])

    background_tasks.add_task(handle_dupes)
    return {"message": "Duplicates resolved successfully"}
