# Self-Healing Data Pipeline 🤖🛠️

An enterprise-grade, autonomous Data Engineering pipeline that uses Agentic AI (LangGraph + Large Language Models) to actively monitor, diagnose, and repair data ingestion failures in real-time.

---

## 📸 Project Showcase

### 1. The Real-Time Dashboard
![Dashboard Viewer](assets/screenshot1.png)
> *The central Vanilla JS/HTML dashboard interacting with the FastAPI backend. Features live health monitoring, anomaly injection controls, and a scrollable real-time data table.*

### 2. Human-In-The-Loop (HITL) Duplicate Resolution
![HITL Output](assets/screenshot2.png)
> *When exact duplicate rows enter the pipeline, the AI halts to prevent data destruction. It triggers a 15-second User Interface modal allowing a human to confidently choose between keeping the New payload or reverting to the Old payload.*

### 3. The LangGraph Agentic Backend
![Agent Terminal Logging](assets/screenshot3.png)
> *Under the hood, LangGraph forces the AI into a strict State Machine. Here, the `Diagnostic Agent` systematically pulls Airflow stack traces, queries PostgreSQL schema info, and mathematically calculates the bad row count.*

### 4. Apache Airflow Execution
![Airflow DAG](assets/screenshot4.png)
> *The Apache Airflow infrastructure tracking the `data_ingestion_pipeline`. If the data violates quality validators, the DAG deliberately fails, alerting the FastAPI backend to wake up the LLM.*

---

## 🚀 Overview
Traditional data pipelines rely on rigid `if/else` alerts, requiring engineers to wake up at 3:00 AM to manually triage Airflow logs and fix broken schemas.
This project introduces a **Self-Healing Layer** using an LLM-powered state machine. When a pipeline crashes, the AI acts as a digital Data Engineer: reading the logs, mathematically diagnosing the root cause, executing the correct `SQL` fix, safely quarantining corrupted data, and independently restarting the Data Pipeline.

## 🏗️ Architecture & Tech Stack
* **Orchestrator:** Apache Airflow *(Running in Docker)*
* **Database:** PostgreSQL *(Running in Docker)*
* **Backend:** FastAPI (Python)
* **AI Orchestrator:** LangGraph + Llama-3 (via Groq API)
* **Frontend:** Vanilla HTML/JS/CSS (No bloat)

## ✨ Key Features
1. **Agentic State Machine:** A heavily constrained LangGraph containing four independent agents:
   * **Monitor Agent:** Pulls logs and execution state.
   * **Diagnostic Agent:** Reasons over the logs to classify the exact failure (Schema Drift, Data Quality, Duplicates).
   * **Fix Agent:** Safely executes mapped tools (e.g., `restore_schema`, `quarantine_bad_rows`).
   * **Escalation Agent:** Reaches out to a human if the AI realizes a fix is too dangerous (e.g. massive volume spikes).
2. **Data Quarantine Engine:** Bad data isn't just deleted—it's preserved in a `quarantine_sales` table for data lineage.
3. **True Duplicate Upserting:** Simulating duplicates physically re-selects exact existing rows to challenge the pipeline validators.

## 🔄 Project Flow
1. Data flows into the FastAPI backend (via UI forms or CSV).
2. Data is inserted into Postgres, and FastAPI triggers the Airflow validation DAG.
3. If an anomaly exists (e.g., negative prices, missing columns), the DAG **fails**.
4. The background process detects the failure and awakens the `langgraph_graph`.
5. The LLM Agents diagnose the Airflow stack trace, execute the valid SQL repair script, and re-trigger Airflow successfully.

## 📂 File Structure
```text
/agents             # Standalone LangGraph LLM agents (monitor, diagnostic, fix, escalation)
/airflow/dags       # Airflow DAG logic (data_ingestion_dag.py)
/api                # FastAPI brain handling routes, background tasks, and DB connections
/database           # Bootstrapping SQL schema (raw_sales, quarantine_sales, audit_log)
/frontend           # Vanilla HTML/JS/CSS Dashboard
/orchestrator       # langgraph_graph.py state machine connecting the agents
```

## ⚙️ Setup & Installation
Because this project utilizes a hybrid Docker/Local architecture, you must start the Docker infrastructure and then run the Python API locally.

1. **Clone the repository** and open your terminal.
2. **Setup Background Infrastructure:**
   ```bash
   docker-compose up -d --build
   ```
   *(This launches PostgreSQL on port 5432 and Apache Airflow on port 8080)*
3. **Add API Keys:** 
   Rename `.env.example` to `.env` and paste your Groq API key:
   `GROQ_API_KEY=your_key_here`
4. **Setup Python Environment:**
   Create a virtual environment and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: .\venv\Scripts\Activate
   pip install -r requirements.txt
   ```
5. **Start the FastAPI Backend:**
   ```bash
   uvicorn api.main:app --reload
   ```
6. **Open the Dashboard:** Open `frontend/index.html` in your web browser.

## 🧪 Simulating Failures
Use the UI Dashboard to actively break your local pipeline:
* **Drop Schema Column:** Deletes the `price` column. Watch the AI run an `ALTER TABLE` to restore it.
* **Inject Bad Data:** Inserts massive randomized negative quantities. Watch the AI move them to Quarantine.
* **Inject Duplicates:** Triggers your Human-In-The-Loop resolution pop-up.
* **Spike Volume:** Inserts 500 rows at once, triggering an anomaly limit that safely escalates without deleting data.