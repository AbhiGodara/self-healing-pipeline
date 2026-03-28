import json
import time
import subprocess
import re
from datetime import datetime
from typing import TypedDict, Literal, List
from langgraph.graph import StateGraph, END
from langchain_groq import ChatGroq
from langchain_core.messages import HumanMessage, SystemMessage
from dotenv import load_dotenv
import os

load_dotenv()

llm = ChatGroq(
    model="llama-3.3-70b-versatile",          # 8b to conserve Groq free-tier tokens
    api_key=os.getenv("GROQ_API_KEY"),
    temperature=0
)

# ── HELPERS ───────────────────────────────────────────────────

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

def safe_int(text: str, default: int = 0) -> int:
    match = re.search(r'\d+', str(text))
    return int(match.group()) if match else default

def ask_llm(system: str, user: str) -> str:
    response = llm.invoke([
        SystemMessage(content=system),
        HumanMessage(content=user)
    ])
    return response.content.strip()

def get_latest_failed_run() -> str:
    output = run_airflow(["airflow", "dags", "list-runs", "-d", "data_ingestion_pipeline"])
    for line in output.split("\n"):
        if "failed" in line:
            parts = [p.strip() for p in line.split("|") if p.strip()]
            if len(parts) >= 2:
                return parts[1]
    return ""

def get_failed_tasks(run_id: str) -> List[str]:
    """Returns list of failed task_ids for a given run."""
    if not run_id:
        return []
    output = run_airflow([
        "airflow", "tasks", "states-for-dag-run",
        "data_ingestion_pipeline", run_id, "--output", "json"
    ])
    failed = []
    try:
        tasks = json.loads(output)
        for t in tasks:
            if t.get("state") == "failed":
                failed.append(t.get("task_id", ""))
    except Exception:
        # Fallback — parse text output
        for line in output.split("\n"):
            if "failed" in line:
                parts = [p.strip() for p in line.split("|") if p.strip()]
                if parts:
                    failed.append(parts[0])
    return failed

def log_reasoning(
    agent_name: str,
    failure_type: str,
    data_snapshot: dict,
    llm_reasoning: str,
    decision: str,
    confidence: str
):
    """Inserts agent reasoning into reasoning_log table."""
    snapshot_json = json.dumps(data_snapshot).replace("'", "''")
    llm_reasoning_safe = llm_reasoning.replace("'", "''")[:2000]
    decision_safe = decision.replace("'", "''")[:50]
    run_sql(f"""
        INSERT INTO reasoning_log
            (agent_name, failure_type, data_snapshot, llm_reasoning, decision, confidence, created_at)
        VALUES (
            '{agent_name}',
            '{failure_type}',
            '{snapshot_json}'::jsonb,
            '{llm_reasoning_safe}',
            '{decision_safe}',
            '{confidence}',
            NOW()
        );
    """)

def log_audit(agent_name: str, action: str, reasoning: str, success: bool):
    action_safe = action.replace("'", "''")[:200]
    reasoning_safe = reasoning.replace("'", "''")[:500]
    run_sql(f"""
        INSERT INTO audit_log (agent_name, action_taken, reasoning, success, created_at)
        VALUES ('{agent_name}', '{action_safe}', '{reasoning_safe}', {'true' if success else 'false'}, NOW());
    """)


# ── STATE ─────────────────────────────────────────────────────

class PipelineState(TypedDict):
    dag_id: str
    status: str

    # Monitor outputs
    failed_run_id: str
    failed_tasks: List[str]
    total_rows: int
    bad_rows: int          # quantity<=0 OR price<=0
    duplicate_rows: int    # duplicate order_ids
    good_rows: int
    schema_ok: bool
    schema_missing_cols: List[str]
    stale_data: bool       # no rows today at all
    volume_anomaly: bool   # too many / too few rows vs average

    # Diagnostic outputs
    error_type: str        # bad_values | duplicates | schema_drift | stale_data | volume_anomaly | unknown
    fix_action: str        # delete_bad_rows | delete_duplicates | restore_schema | retrigger_ingest | escalate
    llm_reasoning: str
    confidence: str

    # Fix outputs
    fix_success: bool
    retry_count: int


# ── NODE 1: MONITOR ───────────────────────────────────────────

def monitor_node(state: PipelineState) -> PipelineState:
    print(f"\n{'█'*60}")
    print(f"  STEP 1: MONITOR")
    print(f"{'█'*60}")

    # ── 1a. Check for failed DAG run ──
    failed_run_id = get_latest_failed_run()
    failed_tasks = get_failed_tasks(failed_run_id) if failed_run_id else []

    # ── 1b. Bad values check (qty<=0 OR price<=0) ──
    bad_rows = safe_int(run_sql(
        "SELECT COUNT(*) FROM raw_sales WHERE quantity <= 0 OR price <= 0;"
    ))

    # ── 1c. Duplicate order_ids today ──
    duplicate_rows = safe_int(run_sql("""
        SELECT COUNT(*) FROM (
            SELECT order_id FROM raw_sales
            WHERE order_date = CURRENT_DATE
            GROUP BY order_id HAVING COUNT(*) > 1
        ) d;
    """))

    # ── 1d. Total & good rows today ──
    total_rows = safe_int(run_sql(
        "SELECT COUNT(*) FROM raw_sales WHERE order_date = CURRENT_DATE;"
    ))
    good_rows = safe_int(run_sql(
        "SELECT COUNT(*) FROM raw_sales WHERE quantity > 0 AND price > 0 AND order_date = CURRENT_DATE;"
    ))

    # ── 1e. Schema check ──
    schema_result = run_sql("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'raw_sales';
    """)
    required_cols = ["order_id", "product_id", "customer_id", "quantity", "price",
                     "category", "region", "order_date"]
    missing_cols = [c for c in required_cols if c not in schema_result]
    schema_ok = len(missing_cols) == 0

    # ── 1f. Stale data check (no rows at all today) ──
    stale_data = (total_rows == 0)

    # ── 1g. Volume anomaly check ──
    avg_result = run_sql("""
        SELECT COALESCE(AVG(daily_count), 0) FROM (
            SELECT order_date, COUNT(*) as daily_count
            FROM raw_sales
            WHERE order_date < CURRENT_DATE
            GROUP BY order_date
            ORDER BY order_date DESC
            LIMIT 7
        ) recent;
    """)
    avg_daily = float(re.search(r'[\d.]+', avg_result).group()) if re.search(r'[\d.]+', avg_result) else 0
    volume_anomaly = False
    if avg_daily > 0 and total_rows > 0:
        ratio = total_rows / avg_daily
        volume_anomaly = ratio > 3.0 or ratio < 0.3

    # ── Print summary ──
    print(f"  → Failed run:      {failed_run_id or 'none'}")
    print(f"  → Failed tasks:    {failed_tasks or 'none'}")
    print(f"  → Total rows:      {total_rows}")
    print(f"  → Bad value rows:  {bad_rows}")
    print(f"  → Duplicate rows:  {duplicate_rows}")
    print(f"  → Good rows:       {good_rows}")
    print(f"  → Schema OK:       {schema_ok} {f'(missing: {missing_cols})' if missing_cols else ''}")
    print(f"  → Stale data:      {stale_data}")
    print(f"  → Volume anomaly:  {volume_anomaly}")

    state["failed_run_id"]      = failed_run_id
    state["failed_tasks"]       = failed_tasks
    state["total_rows"]         = total_rows
    state["bad_rows"]           = bad_rows
    state["duplicate_rows"]     = duplicate_rows
    state["good_rows"]          = good_rows
    state["schema_ok"]          = schema_ok
    state["schema_missing_cols"] = missing_cols
    state["stale_data"]         = stale_data
    state["volume_anomaly"]     = volume_anomaly

    # Incident = any failure signal
    incident = (
        bool(failed_run_id) or
        bad_rows > 0 or
        duplicate_rows > 0 or
        not schema_ok or
        stale_data or
        volume_anomaly
    )

    state["status"] = "incident_detected" if incident else "healthy"

    if incident:
        print(f"\n  ⚠ Incident detected — routing to Diagnostic Agent")
    else:
        print(f"\n  ✓ Pipeline healthy")

    return state


# ── NODE 2: DIAGNOSTIC ────────────────────────────────────────

def diagnostic_node(state: PipelineState) -> PipelineState:
    print(f"\n{'█'*60}")
    print(f"  STEP 2: DIAGNOSTIC")
    print(f"{'█'*60}")

    # Build a rich data snapshot for LLM
    data_snapshot = {
        "failed_tasks":       state["failed_tasks"],
        "bad_rows":           state["bad_rows"],
        "duplicate_rows":     state["duplicate_rows"],
        "good_rows":          state["good_rows"],
        "total_rows":         state["total_rows"],
        "schema_ok":          state["schema_ok"],
        "schema_missing_cols": state["schema_missing_cols"],
        "stale_data":         state["stale_data"],
        "volume_anomaly":     state["volume_anomaly"],
        "failed_run_exists":  bool(state["failed_run_id"]),
    }

    system_prompt = """You are a senior data pipeline diagnostics expert.

You will receive real metrics from a failing pipeline. Your job is to:
1. Identify the PRIMARY failure type
2. Choose the correct fix action
3. Explain your reasoning in one sentence
4. Rate your confidence

Failure types and their fix actions:
- bad_values       → delete_bad_rows        (bad_rows > 0)
- duplicates       → delete_duplicates      (duplicate_rows > 0)  
- schema_drift     → restore_schema         (schema columns missing)
- stale_data       → retrigger_ingest       (total_rows == 0, no data today)
- volume_anomaly   → escalate               (too many/few rows but data looks ok)
- unknown          → escalate               (can't determine cause)

IMPORTANT: If multiple issues exist, pick the most critical one first.
Priority order: schema_drift > bad_values > duplicates > stale_data > volume_anomaly

Reply with ONLY valid JSON, no markdown, no explanation outside JSON:
{
  "error_type": "<one of the 5 types above or unknown>",
  "fix_action": "<one of the 5 fix actions above>",
  "reasoning": "<one sentence explaining why>",
  "confidence": "high|medium|low"
}"""

    user_prompt = f"""Pipeline metrics:
{json.dumps(data_snapshot, indent=2)}

What is the error type and fix action?"""

    raw_response = ask_llm(system_prompt, user_prompt)
    print(f"  → LLM raw response: {raw_response[:300]}")

    # Parse LLM JSON response
    error_type  = "unknown"
    fix_action  = "escalate"
    reasoning   = raw_response
    confidence  = "low"

    try:
        clean = re.search(r'\{.*\}', raw_response, re.DOTALL)
        if clean:
            parsed = json.loads(clean.group())
            error_type  = parsed.get("error_type",  "unknown")
            fix_action  = parsed.get("fix_action",  "escalate")
            reasoning   = parsed.get("reasoning",   raw_response)
            confidence  = parsed.get("confidence",  "low")
    except Exception as e:
        print(f"  ⚠ Could not parse LLM JSON: {e} — falling back to rule-based")
        # Rule-based fallback so we never get stuck
        if not state["schema_ok"]:
            error_type, fix_action, confidence = "schema_drift",    "restore_schema",     "high"
        elif state["bad_rows"] > 0:
            error_type, fix_action, confidence = "bad_values",       "delete_bad_rows",    "high"
        elif state["duplicate_rows"] > 0:
            error_type, fix_action, confidence = "duplicates",       "delete_duplicates",  "high"
        elif state["stale_data"]:
            error_type, fix_action, confidence = "stale_data",       "retrigger_ingest",   "medium"
        elif state["volume_anomaly"]:
            error_type, fix_action, confidence = "volume_anomaly",   "escalate",           "medium"
        reasoning = f"Rule-based fallback: {error_type}"

    state["error_type"]    = error_type
    state["fix_action"]    = fix_action
    state["llm_reasoning"] = reasoning
    state["confidence"]    = confidence

    print(f"  → Error type:  {error_type}")
    print(f"  → Fix action:  {fix_action}")
    print(f"  → Confidence:  {confidence}")
    print(f"  → Reasoning:   {reasoning}")

    # ── Store reasoning in DB ──
    log_reasoning(
        agent_name   = "diagnostic_agent",
        failure_type = error_type,
        data_snapshot= data_snapshot,
        llm_reasoning= reasoning,
        decision     = fix_action,
        confidence   = confidence
    )

    return state


# ── NODE 3: FIX ───────────────────────────────────────────────

def fix_node(state: PipelineState) -> PipelineState:
    print(f"\n{'█'*60}")
    print(f"  STEP 3: FIX")
    print(f"{'█'*60}")

    fix_action = state["fix_action"]
    dag_id     = state["dag_id"]
    success    = False
    action_detail = fix_action

    # ── FIX 1: Delete bad value rows ──────────────────────────
    if fix_action == "delete_bad_rows":
        print(f"  → Deleting {state['bad_rows']} bad-value rows (qty<=0 or price<=0)...")
        run_sql("DELETE FROM raw_sales WHERE quantity <= 0 OR price <= 0;")
        remaining = safe_int(run_sql(
            "SELECT COUNT(*) FROM raw_sales WHERE quantity <= 0 OR price <= 0;"
        ))
        success = remaining == 0
        action_detail = f"delete_bad_rows: removed {state['bad_rows']} rows, {remaining} remaining"
        print(f"  → Remaining bad rows: {remaining} — {'✓' if success else '✗'}")

    # ── FIX 2: Delete duplicate order_ids ─────────────────────
    elif fix_action == "delete_duplicates":
        print(f"  → Removing {state['duplicate_rows']} duplicate order_id groups...")
        # Keep the row with the highest id (latest insert), delete the rest
        run_sql("""
            DELETE FROM raw_sales
            WHERE id NOT IN (
                SELECT MAX(id)
                FROM raw_sales
                WHERE order_date = CURRENT_DATE
                GROUP BY order_id
            )
            AND order_date = CURRENT_DATE
            AND order_id IN (
                SELECT order_id FROM raw_sales
                WHERE order_date = CURRENT_DATE
                GROUP BY order_id HAVING COUNT(*) > 1
            );
        """)
        remaining_dupes = safe_int(run_sql("""
            SELECT COUNT(*) FROM (
                SELECT order_id FROM raw_sales
                WHERE order_date = CURRENT_DATE
                GROUP BY order_id HAVING COUNT(*) > 1
            ) d;
        """))
        success = remaining_dupes == 0
        action_detail = f"delete_duplicates: {remaining_dupes} duplicate groups remaining"
        print(f"  → Remaining duplicate groups: {remaining_dupes} — {'✓' if success else '✗'}")

    # ── FIX 3: Restore missing schema columns ─────────────────
    elif fix_action == "restore_schema":
        missing = state.get("schema_missing_cols", [])
        print(f"  → Restoring missing columns: {missing}")
        col_definitions = {
            "order_id":    "ALTER TABLE raw_sales ADD COLUMN IF NOT EXISTS order_id VARCHAR(50);",
            "product_id":  "ALTER TABLE raw_sales ADD COLUMN IF NOT EXISTS product_id VARCHAR(50);",
            "customer_id": "ALTER TABLE raw_sales ADD COLUMN IF NOT EXISTS customer_id VARCHAR(50);",
            "quantity":    "ALTER TABLE raw_sales ADD COLUMN IF NOT EXISTS quantity INTEGER;",
            "price":       "ALTER TABLE raw_sales ADD COLUMN IF NOT EXISTS price FLOAT;",
            "category":    "ALTER TABLE raw_sales ADD COLUMN IF NOT EXISTS category VARCHAR(50);",
            "region":      "ALTER TABLE raw_sales ADD COLUMN IF NOT EXISTS region VARCHAR(50);",
            "order_date":  "ALTER TABLE raw_sales ADD COLUMN IF NOT EXISTS order_date DATE;",
        }
        for col in missing:
            if col in col_definitions:
                run_sql(col_definitions[col])
                print(f"    ✓ Restored column: {col}")

        # Verify
        schema_result = run_sql("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'raw_sales';
        """)
        still_missing = [c for c in missing if c not in schema_result]
        success = len(still_missing) == 0
        action_detail = f"restore_schema: restored {missing}, still missing: {still_missing}"
        print(f"  → Still missing: {still_missing} — {'✓' if success else '✗'}")

    # ── FIX 4: Re-trigger ingest (stale data) ─────────────────
    elif fix_action == "retrigger_ingest":
        print(f"  → No data today — re-triggering DAG ingest...")
        trigger_output = run_airflow(["airflow", "dags", "trigger", dag_id])
        print(f"  → Trigger output: {trigger_output[:100]}")
        time.sleep(5)
        # Check if a new run started
        runs_output = run_airflow(["airflow", "dags", "list-runs", "-d", dag_id])
        success = "running" in runs_output or "success" in runs_output
        action_detail = "retrigger_ingest: DAG triggered for fresh data ingestion"
        print(f"  → DAG triggered — {'✓' if success else 'waiting...'}")

    else:
        # escalate — nothing to fix here
        print(f"  → Fix action is 'escalate' — skipping auto-fix")
        success = False
        action_detail = f"no_auto_fix: escalated due to {state['error_type']}"

    # ── Post-fix: retrigger DAG (for data fixes) ──────────────
    if success and fix_action in ("delete_bad_rows", "delete_duplicates", "restore_schema"):
        print(f"  → Retriggering DAG after fix...")
        run_airflow(["airflow", "dags", "trigger", dag_id])
        time.sleep(3)

    # ── Log to audit ──────────────────────────────────────────
    log_audit(
        agent_name = "fix_agent",
        action     = action_detail,
        reasoning  = f"error_type={state['error_type']} confidence={state['confidence']} bad_rows={state['bad_rows']} dupes={state['duplicate_rows']}",
        success    = success
    )

    # ── Log reasoning ─────────────────────────────────────────
    log_reasoning(
        agent_name    = "fix_agent",
        failure_type  = state["error_type"],
        data_snapshot = {
            "fix_action":    fix_action,
            "bad_rows":      state["bad_rows"],
            "duplicate_rows": state["duplicate_rows"],
            "schema_missing": state["schema_missing_cols"],
        },
        llm_reasoning = action_detail,
        decision      = "fix_success" if success else "fix_failed",
        confidence    = state["confidence"]
    )

    if success:
        print(f"  ✓ Fix successful — logged to audit + reasoning_log")
    else:
        print(f"  ✗ Fix failed or not applicable")

    state["fix_success"]  = success
    state["retry_count"]  = state.get("retry_count", 0) + 1
    state["status"]       = "fixed" if success else "fix_failed"

    return state


# ── NODE 4: ESCALATION ────────────────────────────────────────

def escalation_node(state: PipelineState) -> PipelineState:
    print(f"\n{'█'*60}")
    print(f"  STEP 4: ESCALATION")
    print(f"{'█'*60}")

    reason = "unknown"
    if state.get("retry_count", 0) >= 2:
        reason = "max_retries_exceeded"
    elif state.get("fix_action") == "escalate":
        reason = f"auto_fix_not_possible_for_{state.get('error_type','unknown')}"
    elif state.get("status") == "fix_failed":
        reason = "fix_attempt_failed"

    print(f"  ⚠ Escalating to human")
    print(f"  → Reason:      {reason}")
    print(f"  → Error type:  {state.get('error_type', 'unknown')}")
    print(f"  → Reasoning:   {state.get('llm_reasoning', '')[:100]}")
    print(f"  → Retries:     {state.get('retry_count', 0)}")
    print(f"  → [In production: Slack/PagerDuty/email alert sent here]")

    log_audit(
        agent_name = "escalation_agent",
        action     = f"escalated_to_human: {reason}",
        reasoning  = f"error_type={state.get('error_type')} retries={state.get('retry_count',0)} llm_confidence={state.get('confidence')}",
        success    = False
    )

    log_reasoning(
        agent_name    = "escalation_agent",
        failure_type  = state.get("error_type", "unknown"),
        data_snapshot = {
            "reason":      reason,
            "retry_count": state.get("retry_count", 0),
            "fix_action":  state.get("fix_action"),
        },
        llm_reasoning = state.get("llm_reasoning", ""),
        decision      = "escalated",
        confidence    = state.get("confidence", "low")
    )

    state["status"] = "escalated"
    return state


# ── NODE 5: SUCCESS ───────────────────────────────────────────

def success_node(state: PipelineState) -> PipelineState:
    print(f"\n{'█'*60}")
    print(f"  SELF-HEALING COMPLETE ✓")
    print(f"{'█'*60}")
    print(f"\n  Summary:")
    print(f"  ├─ DAG:            {state['dag_id']}")
    print(f"  ├─ Status:         {state['status']}")
    print(f"  ├─ Error type:     {state['error_type']}")
    print(f"  ├─ Fix action:     {state['fix_action']}")
    print(f"  ├─ Bad rows:       {state['bad_rows']} removed")
    print(f"  ├─ Duplicate rows: {state['duplicate_rows']} removed")
    print(f"  ├─ Good rows:      {state['good_rows']} preserved")
    print(f"  ├─ Schema OK:      {state['schema_ok']}")
    print(f"  ├─ Confidence:     {state['confidence']}")
    print(f"  ├─ LLM reasoning:  {state['llm_reasoning'][:80]}")
    print(f"  └─ Retries:        {state['retry_count']}")
    return state


# ── ROUTING ───────────────────────────────────────────────────

def route_after_monitor(state: PipelineState) -> Literal["diagnostic", "end"]:
    return "diagnostic" if state["status"] == "incident_detected" else "end"

def route_after_diagnostic(state: PipelineState) -> Literal["fix", "escalate"]:
    return "escalate" if state["fix_action"] == "escalate" else "fix"

def route_after_fix(state: PipelineState) -> Literal["success", "escalate"]:
    if state["fix_success"]:
        return "success"
    elif state.get("retry_count", 0) >= 2:
        return "escalate"
    return "escalate"


# ── GRAPH ─────────────────────────────────────────────────────

def build_graph():
    graph = StateGraph(PipelineState)
    graph.add_node("monitor",    monitor_node)
    graph.add_node("diagnostic", diagnostic_node)
    graph.add_node("fix",        fix_node)
    graph.add_node("escalation", escalation_node)
    graph.add_node("success",    success_node)

    graph.set_entry_point("monitor")

    graph.add_conditional_edges("monitor",    route_after_monitor,    {"diagnostic": "diagnostic", "end": END})
    graph.add_conditional_edges("diagnostic", route_after_diagnostic, {"fix": "fix", "escalate": "escalation"})
    graph.add_conditional_edges("fix",        route_after_fix,        {"success": "success", "escalate": "escalation"})

    graph.add_edge("escalation", END)
    graph.add_edge("success",    END)

    return graph.compile()


# ── MAIN ──────────────────────────────────────────────────────

def run_self_healing_pipeline(dag_id: str = "data_ingestion_pipeline") -> dict:
    print(f"\n{'='*60}")
    print(f"  SELF-HEALING PIPELINE AGENT")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  DAG: {dag_id}")
    print(f"{'='*60}")

    state: PipelineState = {
        "dag_id":              dag_id,
        "status":              "unknown",
        "failed_run_id":       "",
        "failed_tasks":        [],
        "total_rows":          0,
        "bad_rows":            0,
        "duplicate_rows":      0,
        "good_rows":           0,
        "schema_ok":           True,
        "schema_missing_cols": [],
        "stale_data":          False,
        "volume_anomaly":      False,
        "error_type":          "unknown",
        "fix_action":          "",
        "llm_reasoning":       "",
        "confidence":          "low",
        "fix_success":         False,
        "retry_count":         0,
    }

    graph = build_graph()
    final_state = graph.invoke(state)

    print(f"\n{'='*60}")
    print(f"  FINAL STATE: {final_state['status'].upper()}")
    print(f"{'='*60}\n")
    return final_state


# ── QUICK TEST ────────────────────────────────────────────────

if __name__ == "__main__":
    run_self_healing_pipeline("data_ingestion_pipeline")