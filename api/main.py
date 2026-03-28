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
from orchestrator.langgraph_graph import run_self_healing_pipeline, run_sql, run_airflow

app = FastAPI(title="Self-Healing Pipeline API", version="1.0.0")

# Allow React frontend to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── HELPERS ───────────────────────────────────────────────────

def parse_int(text: str) -> int:
    match = re.search(r'\d+', text)
    return int(match.group()) if match else 0

# ── ROUTES ───────────────────────────────────────────────────

@app.get("/")
def root():
    return {"status": "ok", "message": "Self-Healing Pipeline API"}


@app.get("/health")
def get_pipeline_health():
    """Returns current pipeline health — row counts, bad rows, schema status."""
    total = parse_int(run_sql("SELECT COUNT(*) FROM raw_sales;"))
    bad = parse_int(run_sql("SELECT COUNT(*) FROM raw_sales WHERE quantity <= 0 OR price <= 0;"))
    good = total - bad

    schema_result = run_sql("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'raw_sales' ORDER BY ordinal_position;
    """)
    required = ["product_id", "quantity", "price", "sale_date"]
    schema_ok = all(col in schema_result for col in required)

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
            sale_date,
            COUNT(*) as total,
            SUM(CASE WHEN quantity > 0 AND price > 0 THEN 1 ELSE 0 END) as good,
            SUM(CASE WHEN quantity <= 0 OR price <= 0 THEN 1 ELSE 0 END) as bad
        FROM raw_sales
        GROUP BY sale_date
        ORDER BY sale_date DESC
        LIMIT 7;
    """)

    rows = []
    for line in result.strip().split("\n"):
        if "|" in line and "sale_date" not in line and "---" not in line:
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


@app.post("/inject-failure")
def inject_failure(background_tasks: BackgroundTasks):
    """Injects bad data and triggers DAG — for demo/testing."""
    def inject():
        run_sql("""
            INSERT INTO raw_sales (product_id, quantity, price, sale_date)
            SELECT 'BAD-PRODUCT', -99, -1.0, CURRENT_DATE
            FROM generate_series(1,10);
        """)
        run_airflow(["airflow", "dags", "trigger", "data_ingestion_pipeline"])

    background_tasks.add_task(inject)
    return {"message": "Bad data injected and DAG triggered"}