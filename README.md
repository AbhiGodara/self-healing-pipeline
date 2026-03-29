# Self-Healing Data Pipeline 🤖🛠️

An enterprise-grade, autonomous Data Engineering pipeline that uses Agentic AI (LangGraph + Large Language Models) to actively monitor, diagnose, and repair data ingestion failures in real-time.

## 🚀 Overview
Traditional data pipelines rely on rigid `if/else` alerts, requiring engineers to wake up at 3:00 AM to manually triage Airflow logs and fix broken schemas.
This project introduces a **Self-Healing Layer** using an LLM-powered state machine. When a pipeline crashes, the AI acts as a digital Data Engineer: reading the logs, Semantically diagnosing the root cause, executing the correct `SQL` fix, safely quarantining corrupted data, and independently restarting the Data Pipeline.

## 🧠 Architecture
* **Orchestration:** Apache Airflow
* **Database:** PostgreSQL (with explicit Schema and Dead Letter Queues)
* **Backend:** FastAPI (Python)
* **AI Orchestrator:** LangGraph + Llama-3 (via Groq API)
* **Frontend:** Vanilla HTML/JS/CSS (No complex JS frameworks)

## ✨ Key Features
1. **Agentic State Machine:** A heavily constrained LangGraph containing four independent agents:
   * **Monitor Agent:** Pulls logs and execution state.
   * **Diagnostic Agent:** Reasons over the logs to classify the exact failure (Schema Drift, Data Quality, etc).
   * **Fix Agent:** Safely executes mapped tools (e.g., `restore_schema`, `quarantine_bad_rows`).
   * **Escalation Agent:** Reaches out to a human if the AI realizes a fix is too dangerous (e.g. massive volume spikes).
2. **Data Quarantine Engine:** Bad data isn't just deleted—it's preserved in a `quarantine_sales` table for data lineage.
3. **Human-In-The-Loop (HITL):** When true duplicates enter the pipeline, the AI halts and triggers a 15-second User Interface modal asking the human to choose between saving the New payload or reverting to the Old payload. 
4. **Live Dashboard Visualizer:** An interactive frontend to view pipeline health, track AI Audit Logs in real time, upload CSVs, and dynamically "Inject Anomaly" simulations to watch the AI heal them live.

## 📦 Project Setup
1. Clone the repository.
2. Ensure you have Docker and Docker Compose installed.
3. Add your Groq API key to a `.env` file in the root directory: `GROQ_API_KEY=your_key_here`
4. Run `docker-compose up -d --build` to launch Postgres and Airflow.
5. Setup your python environment and run `pip install -r requirements.txt`.
6. Start the API server: `uvicorn api.main:app --reload`.
7. Open `frontend/index.html` in your web browser.

## 🧪 Simulating Failures
Use the UI Dashboard to actively break your local pipeline:
* **Drop Schema Column:** Deletes the `price` column. Watch the AI run an `ALTER TABLE` to restore it.
* **Inject Bad Data:** Inserts massive negative quantities. Watch the AI move them to the Quarantine Table.
* **Inject Duplicates:** Copies real database rows and re-inserts them to trigger your HITL resolution pop-up.
* **Spike Volume:** Inserts 500 rows at once, triggering an anomaly limit that safely escalates without deleting data.