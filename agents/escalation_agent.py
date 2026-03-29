import os
import json
from datetime import datetime
import subprocess

def log_audit(agent_name: str, action: str, reasoning: str, success: bool):
    """Fallback database logging."""
    action_safe = action.replace("'", "''")[:200]
    reasoning_safe = reasoning.replace("'", "''")[:500]
    subprocess.run([
        "docker", "exec",
        "self_healing_pipeline-postgres-1",
        "psql", "-U", "pipeline_user", "-d", "pipeline_db", "-c",
        f"INSERT INTO audit_log (agent_name, action_taken, reasoning, success, created_at) VALUES ('{agent_name}', '{action_safe}', '{reasoning_safe}', {'true' if success else 'false'}, NOW());"
    ], capture_output=True, text=True, timeout=30)

def run_escalation_agent(state: dict) -> dict:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Escalation Agent starting...")
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
    print(f"  → Retries:     {state.get('retry_count', 0)}")
    
    log_audit("escalation_agent", action=f"escalated_to_human: {reason}", reasoning=f"error_type={state.get('error_type')}", success=False)
    state["status"] = "escalated"
    return state
