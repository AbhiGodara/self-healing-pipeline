import json
import time
from datetime import datetime
from typing import TypedDict, Literal
from langgraph.graph import StateGraph, END

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.monitor_agent import run_monitor_agent
from agents.diagnostic_agent import run_diagnostic_agent
from agents.fix_agent import run_fix_agent

# ── STATE ─────────────────────────────────────────────────────
# This is the shared state passed between all agents

class PipelineState(TypedDict):
    dag_id: str
    incident_report: dict
    diagnosis: dict
    fix_report: dict
    status: str
    retry_count: int
    timestamp: str

# ── NODES (one per agent) ─────────────────────────────────────

def monitor_node(state: PipelineState) -> PipelineState:
    """Runs the Monitor Agent and updates state with incident report."""
    print(f"\n{'█'*60}")
    print(f"  STEP 1: MONITOR AGENT")
    print(f"{'█'*60}")

    report = run_monitor_agent(state["dag_id"])
    state["incident_report"] = report

    if report.get("status") == "incident_detected":
        state["status"] = "incident_detected"
        print(f"\n  ⚠ Incident detected — routing to Diagnostic Agent")
    else:
        state["status"] = "healthy"
        print(f"\n  ✓ Pipeline healthy — no action needed")

    return state


def diagnostic_node(state: PipelineState) -> PipelineState:
    """Runs the Diagnostic Agent and updates state with diagnosis."""
    print(f"\n{'█'*60}")
    print(f"  STEP 2: DIAGNOSTIC AGENT")
    print(f"{'█'*60}")

    diagnosis = run_diagnostic_agent(state["incident_report"])
    state["diagnosis"] = diagnosis

    fix_action = diagnosis.get("fix_action", "escalate")
    print(f"\n  → Fix action determined: {fix_action}")

    return state


def fix_node(state: PipelineState) -> PipelineState:
    """Runs the Fix Agent and updates state with fix report."""
    print(f"\n{'█'*60}")
    print(f"  STEP 3: FIX AGENT")
    print(f"{'█'*60}")

    fix_report = run_fix_agent(state["diagnosis"])
    state["fix_report"] = fix_report
    state["retry_count"] = state.get("retry_count", 0) + 1

    if fix_report.get("success"):
        state["status"] = "fixed"
        print(f"\n  ✓ Fix applied successfully")
    else:
        state["status"] = "fix_failed"
        print(f"\n  ✗ Fix failed")

    return state


def escalation_node(state: PipelineState) -> PipelineState:
    """Handles cases the agent can't fix — logs and alerts."""
    print(f"\n{'█'*60}")
    print(f"  STEP 4: ESCALATION")
    print(f"{'█'*60}")

    reason = "unknown"
    if state.get("retry_count", 0) >= 2:
        reason = "max retries exceeded"
    elif state["diagnosis"].get("fix_action") == "escalate":
        reason = "agent determined human intervention needed"
    elif state["status"] == "fix_failed":
        reason = "fix attempt failed"

    print(f"\n  ⚠ Escalating to human. Reason: {reason}")
    print(f"  → dag_id: {state['dag_id']}")
    print(f"  → error_type: {state['diagnosis'].get('error_type', 'unknown')}")
    print(f"  → root_cause: {state['diagnosis'].get('root_cause', 'unknown')}")
    print(f"\n  [In production: send Slack/email alert here]")

    state["status"] = "escalated"
    return state


def success_node(state: PipelineState) -> PipelineState:
    """Final node — prints summary of the self-healing cycle."""
    print(f"\n{'█'*60}")
    print(f"  SELF-HEALING COMPLETE")
    print(f"{'█'*60}")

    duration = datetime.now().strftime('%H:%M:%S')
    print(f"\n  Summary:")
    print(f"  ├─ DAG:        {state['dag_id']}")
    print(f"  ├─ Status:     {state['status']}")
    print(f"  ├─ Error type: {state['diagnosis'].get('error_type', 'N/A')}")
    print(f"  ├─ Fix:        {state['fix_report'].get('fix_applied', 'N/A')}")
    print(f"  ├─ Retries:    {state.get('retry_count', 0)}")
    print(f"  └─ Time:       {duration}")

    return state


# ── ROUTING LOGIC ─────────────────────────────────────────────

def route_after_monitor(state: PipelineState) -> Literal["diagnostic", "end"]:
    """After monitoring: go to diagnostic if incident, else end."""
    if state["status"] == "incident_detected":
        return "diagnostic"
    return "end"


def route_after_diagnostic(state: PipelineState) -> Literal["fix", "escalate"]:
    """After diagnosis: fix if possible, escalate if not."""
    fix_action = state["diagnosis"].get("fix_action", "escalate")
    if fix_action == "escalate":
        return "escalate"
    return "fix"


def route_after_fix(state: PipelineState) -> Literal["success", "escalate", "monitor"]:
    """After fix: verify success, escalate if failed, retry if needed."""
    if state["fix_report"].get("success"):
        return "success"
    elif state.get("retry_count", 0) >= 2:
        return "escalate"
    else:
        # Retry — go back to monitor to re-check
        print("\n  → Fix unsuccessful, retrying...")
        time.sleep(5)
        return "monitor"


# ── BUILD THE GRAPH ───────────────────────────────────────────

def build_graph():
    graph = StateGraph(PipelineState)

    # Add nodes
    graph.add_node("monitor", monitor_node)
    graph.add_node("diagnostic", diagnostic_node)
    graph.add_node("fix", fix_node)
    graph.add_node("escalation", escalation_node)
    graph.add_node("success", success_node)

    # Set entry point
    graph.set_entry_point("monitor")

    # Add conditional edges
    graph.add_conditional_edges("monitor", route_after_monitor, {
        "diagnostic": "diagnostic",
        "end": END
    })

    graph.add_conditional_edges("diagnostic", route_after_diagnostic, {
        "fix": "fix",
        "escalate": "escalation"
    })

    graph.add_conditional_edges("fix", route_after_fix, {
        "success": "success",
        "escalate": "escalation",
        "monitor": "monitor"
    })

    graph.add_edge("escalation", END)
    graph.add_edge("success", END)

    return graph.compile()


# ── MAIN ──────────────────────────────────────────────────────

def run_self_healing_pipeline(dag_id: str = "data_ingestion_pipeline"):
    """Entry point — runs the full self-healing cycle."""
    print(f"\n{'='*60}")
    print(f"  SELF-HEALING PIPELINE AGENT")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  DAG: {dag_id}")
    print(f"{'='*60}")

    initial_state: PipelineState = {
        "dag_id": dag_id,
        "incident_report": {},
        "diagnosis": {},
        "fix_report": {},
        "status": "unknown",
        "retry_count": 0,
        "timestamp": datetime.now().isoformat()
    }

    graph = build_graph()
    final_state = graph.invoke(initial_state)

    print(f"\n{'='*60}")
    print(f"  FINAL STATE: {final_state['status'].upper()}")
    print(f"{'='*60}\n")

    return final_state


if __name__ == "__main__":
    # Inject failure first so there's something to fix
    import subprocess
    print("Injecting bad data for demo...")
    subprocess.run([
        "docker", "exec",
        "self_healing_pipeline-postgres-1",
        "psql", "-U", "pipeline_user", "-d", "pipeline_db", "-c",
        "INSERT INTO raw_sales (product_id, quantity, price, sale_date) SELECT 'BAD-PRODUCT', -99, -1.0, CURRENT_DATE FROM generate_series(1,10);"
    ], capture_output=True)

    subprocess.run([
        "docker", "exec",
        "self_healing_pipeline-airflow-webserver-1",
        "airflow", "dags", "trigger", "data_ingestion_pipeline"
    ], capture_output=True)

    print("Waiting 20 seconds for DAG to fail...")
    time.sleep(20)

    # Run the full self-healing cycle
    run_self_healing_pipeline("data_ingestion_pipeline")