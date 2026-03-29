import os
import json
import subprocess
import time
import re
from datetime import datetime
from dotenv import load_dotenv
from langchain_groq import ChatGroq
from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage

load_dotenv()

llm = ChatGroq(
    model="llama-3.3-70b-versatile",
    api_key=os.getenv("GROQ_API_KEY"),
    temperature=0
)

# ── TOOLS ────────────────────────────────────────────────────

@tool
def quarantine_bad_rows() -> str:
    """Moves rows with negative quantity or price to quarantine table."""
    try:
        result = subprocess.run([
            "docker", "exec",
            "self_healing_pipeline-postgres-1",
            "psql", "-U", "pipeline_user", "-d", "pipeline_db", "-c",
            """
            CREATE TABLE IF NOT EXISTS quarantine_sales (LIKE raw_sales INCLUDING ALL);
            INSERT INTO quarantine_sales SELECT * FROM raw_sales WHERE quantity <= 0 OR price <= 0;
            DELETE FROM raw_sales WHERE quantity <= 0 OR price <= 0;
            """
        ], capture_output=True, text=True, timeout=30)

        # Verify deletion worked
        verify = subprocess.run([
            "docker", "exec",
            "self_healing_pipeline-postgres-1",
            "psql", "-U", "pipeline_user", "-d", "pipeline_db", "-c",
            "SELECT COUNT(*) as remaining_bad_rows FROM raw_sales WHERE quantity <= 0 OR price <= 0;"
        ], capture_output=True, text=True, timeout=30)

        return f"Delete result: {result.stdout}\nVerification: {verify.stdout}"

    except Exception as e:
        return f"Exception: {str(e)}"


@tool
def restore_schema() -> str:
    """Restores missing columns in raw_sales table."""
    try:
        result = subprocess.run([
            "docker", "exec",
            "self_healing_pipeline-postgres-1",
            "psql", "-U", "pipeline_user", "-d", "pipeline_db", "-c",
            """
            ALTER TABLE raw_sales ADD COLUMN IF NOT EXISTS price FLOAT;
            ALTER TABLE raw_sales ADD COLUMN IF NOT EXISTS quantity INTEGER;
            ALTER TABLE raw_sales ADD COLUMN IF NOT EXISTS product_id VARCHAR(50);
            ALTER TABLE raw_sales ADD COLUMN IF NOT EXISTS order_date DATE;
            SELECT 'Schema restored successfully' as result;
            """
        ], capture_output=True, text=True, timeout=30)

        return result.stdout

    except Exception as e:
        return f"Exception: {str(e)}"


@tool
def retrigger_dag(dag_id: str) -> str:
    """Triggers a fresh DAG run after fix is applied."""
    try:
        result = subprocess.run([
            "docker", "exec",
            "self_healing_pipeline-airflow-webserver-1",
            "airflow", "dags", "trigger", dag_id
        ], capture_output=True, text=True, timeout=30)

        time.sleep(10)  # wait for run to start

        # Check if new run started
        check = subprocess.run([
            "docker", "exec",
            "self_healing_pipeline-airflow-webserver-1",
            "airflow", "dags", "list-runs", "-d", dag_id
        ], capture_output=True, text=True, timeout=30)

        # Get the latest run state
        for line in check.stdout.split("\n"):
            if "success" in line or "running" in line or "failed" in line:
                parts = [p.strip() for p in line.split("|") if p.strip()]
                if len(parts) >= 3:
                    return f"DAG triggered. Latest run: run_id={parts[1]} state={parts[2]}"

        return f"DAG triggered.\n{result.stdout}"

    except Exception as e:
        return f"Exception: {str(e)}"


@tool
def log_fix_to_audit(action: str, success: bool, reasoning: str) -> str:
    """Logs the fix action to the audit_log table."""
    try:
        success_val = "true" if success else "false"
        # Sanitize inputs to prevent SQL issues
        action = action.replace("'", "''")
        reasoning = reasoning.replace("'", "''")

        result = subprocess.run([
            "docker", "exec",
            "self_healing_pipeline-postgres-1",
            "psql", "-U", "pipeline_user", "-d", "pipeline_db", "-c",
            f"""INSERT INTO audit_log (agent_name, action_taken, reasoning, success, created_at)
            VALUES ('fix_agent', '{action}', '{reasoning}', {success_val}, NOW())
            RETURNING id, created_at;"""
        ], capture_output=True, text=True, timeout=30)

        return f"Logged to audit: {result.stdout}"

    except Exception as e:
        return f"Exception: {str(e)}"


@tool
def verify_pipeline_health(dag_id: str) -> str:
    """Checks if the pipeline is healthy after the fix."""
    try:
        # Check bad rows
        db_check = subprocess.run([
            "docker", "exec",
            "self_healing_pipeline-postgres-1",
            "psql", "-U", "pipeline_user", "-d", "pipeline_db", "-c",
            "SELECT COUNT(*) as bad_rows FROM raw_sales WHERE quantity <= 0 OR price <= 0;"
        ], capture_output=True, text=True, timeout=30)

        # Check latest run state
        run_check = subprocess.run([
            "docker", "exec",
            "self_healing_pipeline-airflow-webserver-1",
            "airflow", "dags", "list-runs", "-d", dag_id
        ], capture_output=True, text=True, timeout=30)

        latest_state = "unknown"
        for line in run_check.stdout.split("\n"):
            if "success" in line or "running" in line or "failed" in line:
                parts = [p.strip() for p in line.split("|") if p.strip()]
                if len(parts) >= 3:
                    latest_state = parts[2]
                    break

        return f"DB health:\n{db_check.stdout}\nLatest run state: {latest_state}"

    except Exception as e:
        return f"Exception: {str(e)}"


# ── FIX AGENT ────────────────────────────────────────────────

tools = [quarantine_bad_rows, restore_schema, retrigger_dag, log_fix_to_audit, verify_pipeline_health]
llm_with_tools = llm.bind_tools(tools)


def run_fix_agent(diagnosis: dict) -> dict:
    """
    Takes the Diagnostic Agent's diagnosis and applies the fix.
    Returns a fix report with success/failure status.
    """
    print(f"\n{'='*60}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Fix Agent starting...")
    print(f"{'='*60}")

    dag_id = diagnosis.get("dag_id", "data_ingestion_pipeline")
    fix_action = diagnosis.get("fix_action", "escalate")
    error_type = diagnosis.get("error_type", "unknown")

    system_prompt = """You are a pipeline fix agent. You apply fixes based on a diagnosis.

Fix actions by error type:
- fix_action='delete_bad_rows': call quarantine_bad_rows, then retrigger_dag, then verify_pipeline_health
- fix_action='restore_schema': call restore_schema, then retrigger_dag, then verify_pipeline_health
- fix_action='restart_task': call retrigger_dag directly, then verify_pipeline_health
- fix_action='escalate': only call log_fix_to_audit with action='escalated_to_human'

After fixing:
1. Always call log_fix_to_audit to record what you did
2. Always call verify_pipeline_health to confirm fix worked

Return ONLY this JSON:
{
  "dag_id": "<dag_id>",
  "fix_applied": "<what you did>",
  "fix_action": "<action taken>",
  "success": true or false,
  "verification": "passed" or "failed" or "pending",
  "new_run_state": "success" or "running" or "failed" or "unknown",
  "timestamp": "<now>",
  "needs_escalation": true or false
}"""

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=f"""Apply fix for this diagnosis:
dag_id: {dag_id}
fix_action: {fix_action}
error_type: {error_type}
bad_row_count: {diagnosis.get('bad_row_count', 0)}
root_cause: {diagnosis.get('root_cause', 'unknown')}

Apply the fix, verify it worked, log it to audit, and return your fix report JSON.""")
    ]

    MAX_ITERATIONS = 8
    iteration = 0

    while iteration < MAX_ITERATIONS:
        response = llm_with_tools.invoke(messages)
        messages.append(response)
        iteration += 1

        if not response.tool_calls:
            break

        for tool_call in response.tool_calls:
            tool_name = tool_call["name"]
            tool_args = tool_call["args"]

            print(f"  → [{iteration}/{MAX_ITERATIONS}] Calling tool: {tool_name}")

            tool_fn = next((t for t in tools if t.name == tool_name), None)
            tool_result = tool_fn.invoke(tool_args) if tool_fn else f"Tool {tool_name} not found"
            time.sleep(4)

            messages.append(ToolMessage(
                content=str(tool_result),
                tool_call_id=tool_call["id"]
            ))

    if iteration >= MAX_ITERATIONS:
        print("  ⚠ Max iterations reached — forcing final response")
        messages.append(HumanMessage(content="Reached limit. Output your fix report JSON now."))
        response = llm_with_tools.invoke(messages)

    final_text = response.content
    print(f"\n[Fix Agent Report]:\n{final_text}")

    try:
        json_match = re.search(r'\{.*\}', final_text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())
    except Exception as e:
        print(f"Could not parse JSON: {e}")

    return {"error": "Could not parse fix report", "raw": final_text}


if __name__ == "__main__":
    # Simulate receiving a diagnosis from Diagnostic Agent
    test_diagnosis = {
        "dag_id": "data_ingestion_pipeline",
        "run_id": "",
        "failed_task": "validate_data",
        "root_cause": "Bad rows with negative quantity/price found in raw_sales",
        "error_type": "data_quality",
        "fix_action": "delete_bad_rows",
        "bad_row_count": 10,
        "confidence": "high"
    }

    fix_report = run_fix_agent(test_diagnosis)
    print(f"\nFinal Fix Report: {json.dumps(fix_report, indent=2)}")