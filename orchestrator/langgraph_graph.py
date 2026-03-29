import os
import sys
from datetime import datetime
from typing import TypedDict, Literal

# Ensure agents folder is in path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.monitor_agent import run_monitor_agent
from agents.diagnostic_agent import run_diagnostic_agent
from agents.fix_agent import run_fix_agent
from agents.escalation_agent import run_escalation_agent

from langgraph.graph import StateGraph, END

# ── STATE ─────────────────────────────────────────────────────
class PipelineState(TypedDict):
    dag_id: str
    status: str
    retry_count: int
    
    # Context dictionary passing between agents
    incident_report: dict 
    diagnosis: dict
    fix_report: dict

# ── NODES ─────────────────────────────────────────────────────

def monitor_node(state: PipelineState) -> PipelineState:
    print(f"\n{'█'*60}\n  STEP 1: MONITOR via agents/monitor_agent.py\n{'█'*60}")
    incident_report = run_monitor_agent(state["dag_id"])
    
    if incident_report and incident_report.get("status") == "incident_detected":
        state["status"] = "incident_detected"
        state["incident_report"] = incident_report
    else:
        state["status"] = "healthy"
        state["incident_report"] = incident_report
        
    return state

def diagnostic_node(state: PipelineState) -> PipelineState:
    print(f"\n{'█'*60}\n  STEP 2: DIAGNOSTIC via agents/diagnostic_agent.py\n{'█'*60}")
    diagnosis = run_diagnostic_agent(state["incident_report"])
    
    state["diagnosis"] = diagnosis
    # error_type and fix_action are derived from diagnosis
    return state

def fix_node(state: PipelineState) -> PipelineState:
    print(f"\n{'█'*60}\n  STEP 3: FIX via agents/fix_agent.py\n{'█'*60}")
    fix_report = run_fix_agent(state["diagnosis"])
    
    state["fix_report"] = fix_report
    state["retry_count"] = state.get("retry_count", 0) + 1
    
    if fix_report.get("success") == True:
        state["status"] = "fixed"
    else:
        state["status"] = "fix_failed"
        
    return state

def escalation_node(state: PipelineState) -> PipelineState:
    print(f"\n{'█'*60}\n  STEP 4: ESCALATION via agents/escalation_agent.py\n{'█'*60}")
    # Flatten state for escalation agent
    flat_state = {
        "retry_count": state.get("retry_count", 0),
        "status": state.get("status", "escalated"),
        "error_type": state.get("diagnosis", {}).get("error_type", "unknown"),
        "fix_action": state.get("diagnosis", {}).get("fix_action", "escalate")
    }
    
    run_escalation_agent(flat_state)
    state["status"] = "escalated"
    return state

def success_node(state: PipelineState) -> PipelineState:
    print(f"\n{'█'*60}\n  SELF-HEALING COMPLETE ✓\n{'█'*60}")
    print(f"Status: {state['status']}")
    print(f"Diagnosis: {state.get('diagnosis', {}).get('root_cause')}")
    print(f"Fix Applied: {state.get('fix_report', {}).get('fix_applied')}")
    return state

# ── ROUTING ───────────────────────────────────────────────────

def route_after_monitor(state: PipelineState) -> Literal["diagnostic", "end"]:
    return "diagnostic" if state["status"] == "incident_detected" else "end"

def route_after_diagnostic(state: PipelineState) -> Literal["fix", "escalate"]:
    return "escalate" if state.get("diagnosis", {}).get("fix_action") == "escalate" else "fix"

def route_after_fix(state: PipelineState) -> Literal["success", "escalate"]:
    if state["status"] == "fixed":
        return "success"
    elif state.get("retry_count", 0) >= 2:
        return "escalate"
    return "escalate"

# ── GRAPH COMPILATION ─────────────────────────────────────────

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
    print(f"  SELF-HEALING PIPELINE AGENT (Refactored)")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  DAG: {dag_id}")
    print(f"{'='*60}")

    state: PipelineState = {
        "dag_id": dag_id,
        "status": "unknown",
        "retry_count": 0,
        "incident_report": {},
        "diagnosis": {},
        "fix_report": {}
    }

    graph = build_graph()
    final_state = graph.invoke(state)

    print(f"\n{'='*60}")
    print(f"  FINAL STATE: {final_state['status'].upper()}")
    print(f"{'='*60}\n")
    
    # Return mapping for API expectations
    return {
        "status": final_state["status"],
        "fix_action": final_state.get("diagnosis", {}).get("fix_action"),
        "bad_rows": final_state.get("diagnosis", {}).get("bad_row_count", 0),
        "good_rows": 0 # simplified fallback
    }

if __name__ == "__main__":
    run_self_healing_pipeline("data_ingestion_pipeline")