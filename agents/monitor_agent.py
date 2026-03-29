import os
import json
import subprocess
import time
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
def get_recent_dag_runs(dag_id: str) -> str:
    """Fetches the last 5 runs of a DAG and their states from Airflow."""
    try:
        result = subprocess.run([
            "docker", "exec",
            "self_healing_pipeline-airflow-webserver-1",
            "airflow", "dags", "list-runs",
            "-d", dag_id,
        ], capture_output=True, text=True, timeout=30)

        lines = result.stdout.strip().split("\n")
        # Find lines with actual run data (skip headers/separators)
        runs = []
        for line in lines:
            if "success" in line or "failed" in line or "running" in line:
                parts = [p.strip() for p in line.split("|") if p.strip()]
                if len(parts) >= 3:
                    runs.append({
                        "dag_id": parts[0] if len(parts) > 0 else "",
                        "run_id": parts[1] if len(parts) > 1 else "",
                        "state": parts[2] if len(parts) > 2 else "",
                    })

        if not runs:
            return "No runs found."

        # Format clearly for the LLM
        output = "Recent DAG runs:\n"
        for r in runs[:5]:
            output += f"- run_id: {r['run_id']} | state: {r['state']}\n"
        return output

    except Exception as e:
        return f"Exception: {str(e)}"


@tool
def get_failed_task_details(dag_id: str, run_id: str) -> str:
    """Gets which specific task failed in a given DAG run."""
    try:
        result = subprocess.run([
            "docker", "exec",
            "self_healing_pipeline-airflow-webserver-1",
            "airflow", "tasks", "states-for-dag-run",
            dag_id, run_id,
            "--output", "json"
        ], capture_output=True, text=True, timeout=30)

        if result.returncode != 0:
            return f"Error: {result.stderr}"

        return result.stdout

    except Exception as e:
        return f"Exception: {str(e)}"


@tool
def get_task_logs(dag_id: str, task_id: str, run_id: str) -> str:
    """Reads the actual error logs from a failed task."""
    try:
        result = subprocess.run([
            "docker", "exec",
            "self_healing_pipeline-airflow-webserver-1",
            "airflow", "tasks", "logs",
            dag_id, task_id, run_id
        ], capture_output=True, text=True, timeout=30)

        # Return last 100 lines — enough for diagnosis
        lines = result.stdout.split("\n")
        return "\n".join(lines[-100:])

    except Exception as e:
        return f"Exception: {str(e)}"


@tool
def check_database_health() -> str:
    """Checks if Postgres is up and raw_sales table has data issues."""
    try:
        result = subprocess.run([
            "docker", "exec",
            "self_healing_pipeline-postgres-1",
            "psql", "-U", "pipeline_user", "-d", "pipeline_db",
            "-c", """
                SELECT
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END) as bad_quantity_rows,
                    SUM(CASE WHEN price <= 0 THEN 1 ELSE 0 END) as bad_price_rows,
                    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) as null_product_rows
                FROM raw_sales
                WHERE order_date = CURRENT_DATE;
            """
        ], capture_output=True, text=True, timeout=30)

        return result.stdout if result.returncode == 0 else f"DB Error: {result.stderr}"

    except Exception as e:
        return f"Exception: {str(e)}"


# ── MONITOR AGENT ─────────────────────────────────────────────

tools = [get_recent_dag_runs, get_failed_task_details, get_task_logs, check_database_health]
llm_with_tools = llm.bind_tools(tools)

def run_monitor_agent(dag_id: str = "data_ingestion_pipeline") -> dict:
    """
    Main monitor agent loop. Checks pipeline health and returns
    a structured incident report if something is wrong.
    """
    print(f"\n{'='*60}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Monitor Agent starting...")
    print(f"{'='*60}")

    system_prompt = """You are a pipeline monitor. Use the tools to check DAG health.

            Steps:
            1. Call get_recent_dag_runs for the given dag_id
            2. If any run has state=failed, call get_failed_task_details with the EXACT run_id from step 1
            3. Call get_task_logs with the EXACT run_id and failed task name from step 2
            4. Call check_database_health

            Then return ONLY this JSON with real values from the tool results:
            {
            "status": "healthy" or "incident_detected",
            "dag_id": "<dag_id>",
            "run_id": "<exact run_id from tool results>",
            "failed_task": "<exact task name from tool results>",
            "error_summary": "<one line>",
            "error_type": "data_quality" or "schema_error" or "connection_error" or "unknown",
            "bad_row_count": <number>,
            "timestamp": "<now>",
            "needs_fix": true or false
            }

            IMPORTANT: Use ONLY values you actually received from tools. Never invent run_ids or task names."""

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=f"Check the health of DAG: {dag_id}. Investigate any failures and return your incident report JSON.")
    ]

    # Agentic loop — keep going until no more tool calls
    MAX_ITERATION=6
    iteration=0
    while iteration<MAX_ITERATION:
        response = llm_with_tools.invoke(messages)
        messages.append(response)
        iteration+=1

        # If no tool calls, agent is done
        if not response.tool_calls:
            break

        # Execute each tool call
        for tool_call in response.tool_calls:
            tool_name = tool_call["name"]
            tool_args = tool_call["args"]

            print(f"  → Calling tool: {tool_name}({tool_args})")

            # Find and run the matching tool
            tool_fn = next((t for t in tools if t.name == tool_name), None)
            if tool_fn:
                tool_result = tool_fn.invoke(tool_args)
                time.sleep(4)
            else:
                tool_result = f"Tool {tool_name} not found"

            messages.append(ToolMessage(
                content=str(tool_result),
                tool_call_id=tool_call["id"]
            ))

    if iteration >= MAX_ITERATION:
        print("  ⚠ Max iterations reached — forcing final response")
        messages.append(HumanMessage(content="You have reached the tool call limit. Based on what you've gathered so far, output your final JSON incident report now. Do not call any more tools."))
        response = llm_with_tools.invoke(messages)

    # Parse the final JSON response
    final_text = response.content
    print(f"\n[Monitor Agent Report]:\n{final_text}")

    try:
        # Extract JSON from response
        import re
        json_match = re.search(r'\{.*\}', final_text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())
    except Exception as e:
        print(f"Could not parse JSON: {e}")

    return {"status": "error", "message": final_text}


if __name__ == "__main__":
    report = run_monitor_agent("data_ingestion_pipeline")
    print(f"\nFinal Report: {json.dumps(report, indent=2)}")