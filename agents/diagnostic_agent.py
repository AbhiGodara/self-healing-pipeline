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
    model="llama-3.1-8b-instant",
    api_key=os.getenv("GROQ_API_KEY"),
    temperature=0
)

# ── TOOLS ────────────────────────────────────────────────────

@tool
def read_airflow_task_logs(dag_id: str, task_id: str, run_id: str) -> str:
    """Reads the raw logs from a failed Airflow task."""
    try:
        result = subprocess.run([
            "docker", "exec",
            "self_healing_pipeline-airflow-webserver-1",
            "airflow", "tasks", "logs",
            dag_id, task_id, run_id
        ], capture_output=True, text=True, timeout=30)

        lines = result.stdout.split("\n")
        # Focus on error lines
        error_lines = [l for l in lines if any(
            kw in l.lower() for kw in ["error", "exception", "failed", "raise", "traceback", "valueerror"]
        )]
        if error_lines:
            return "Error lines from logs:\n" + "\n".join(error_lines[-30:])
        return "Last 50 log lines:\n" + "\n".join(lines[-50:])

    except Exception as e:
        return f"Exception: {str(e)}"


@tool
def check_bad_rows_in_db() -> str:
    """Checks raw_sales table for data quality issues."""
    try:
        result = subprocess.run([
            "docker", "exec",
            "self_healing_pipeline-postgres-1",
            "psql", "-U", "pipeline_user", "-d", "pipeline_db", "-c",
            """SELECT
                COUNT(*) as total_rows,
                SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END) as bad_quantity,
                SUM(CASE WHEN price <= 0 THEN 1 ELSE 0 END) as bad_price,
                SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) as null_products
            FROM raw_sales WHERE sale_date = CURRENT_DATE;"""
        ], capture_output=True, text=True, timeout=30)

        return result.stdout

    except Exception as e:
        return f"Exception: {str(e)}"


@tool
def check_table_schema() -> str:
    """Checks if raw_sales table has all required columns."""
    try:
        result = subprocess.run([
            "docker", "exec",
            "self_healing_pipeline-postgres-1",
            "psql", "-U", "pipeline_user", "-d", "pipeline_db", "-c",
            """SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'raw_sales'
            ORDER BY ordinal_position;"""
        ], capture_output=True, text=True, timeout=30)

        return result.stdout

    except Exception as e:
        return f"Exception: {str(e)}"


@tool
def get_latest_failed_run_id(dag_id: str) -> str:
    """Gets the run_id of the most recent failed DAG run."""
    try:
        result = subprocess.run([
            "docker", "exec",
            "self_healing_pipeline-airflow-webserver-1",
            "airflow", "dags", "list-runs", "-d", dag_id
        ], capture_output=True, text=True, timeout=30)

        for line in result.stdout.split("\n"):
            if "failed" in line:
                parts = [p.strip() for p in line.split("|") if p.strip()]
                if len(parts) >= 2:
                    return f"Latest failed run_id: {parts[1]}"

        return "No failed runs found."

    except Exception as e:
        return f"Exception: {str(e)}"


# ── DIAGNOSTIC AGENT ─────────────────────────────────────────

tools = [read_airflow_task_logs, check_bad_rows_in_db, check_table_schema, get_latest_failed_run_id]
llm_with_tools = llm.bind_tools(tools)


def run_diagnostic_agent(incident_report: dict) -> dict:
    """
    Takes the Monitor Agent's incident report and diagnoses the root cause.
    Returns a diagnosis with recommended fix action.
    """
    print(f"\n{'='*60}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Diagnostic Agent starting...")
    print(f"{'='*60}")

    dag_id = incident_report.get("dag_id", "data_ingestion_pipeline")
    failed_task = incident_report.get("failed_task", "validate_data")
    run_id = incident_report.get("run_id", "")

    system_prompt = """You are a pipeline diagnostic agent. You receive an incident report and find the root cause.

Steps:
1. Call get_latest_failed_run_id to get the real run_id
2. Call read_airflow_task_logs with dag_id, task_id='validate_data', and the run_id from step 1
3. Call check_bad_rows_in_db to see data quality issues
4. Call check_table_schema to verify all columns exist

Then return ONLY this JSON:
{
  "dag_id": "<dag_id>",
  "run_id": "<exact run_id>",
  "failed_task": "validate_data",
  "root_cause": "<one sentence explaining why it failed>",
  "error_type": "data_quality" or "schema_error" or "connection_error" or "unknown",
  "fix_action": "delete_bad_rows" or "restore_schema" or "restart_task" or "escalate",
  "bad_row_count": <number from db check>,
  "confidence": "high" or "medium" or "low",
  "timestamp": "<now>"
}

Use ONLY real values from tool results. Never guess."""

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=f"""Diagnose this incident:
dag_id: {dag_id}
failed_task: {failed_task}
run_id: {run_id}
error_type: {incident_report.get('error_type')}

Investigate and return your diagnosis JSON.""")
    ]

    MAX_ITERATIONS = 6
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
        messages.append(HumanMessage(content="Reached tool limit. Output your diagnosis JSON now based on what you found."))
        response = llm_with_tools.invoke(messages)

    final_text = response.content
    print(f"\n[Diagnostic Agent Report]:\n{final_text}")

    try:
        json_match = re.search(r'\{.*\}', final_text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())
    except Exception as e:
        print(f"Could not parse JSON: {e}")

    return {"error": "Could not parse diagnosis", "raw": final_text}


if __name__ == "__main__":
    # Simulate receiving a report from Monitor Agent
    test_incident = {
        "status": "incident_detected",
        "dag_id": "data_ingestion_pipeline",
        "run_id": "",
        "failed_task": "validate_data",
        "error_type": "data_quality",
        "needs_fix": True
    }

    diagnosis = run_diagnostic_agent(test_incident)
    print(f"\nFinal Diagnosis: {json.dumps(diagnosis, indent=2)}")