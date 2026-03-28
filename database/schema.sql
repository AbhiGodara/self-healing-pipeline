-- E-commerce sales table
CREATE TABLE IF NOT EXISTS raw_sales (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50),
    product_id VARCHAR(50),
    customer_id VARCHAR(50),
    quantity INTEGER,
    price FLOAT,
    category VARCHAR(50),
    region VARCHAR(50),
    order_date DATE,
    ingested_at TIMESTAMP DEFAULT NOW()
);

-- Pipeline health snapshots
CREATE TABLE IF NOT EXISTS pipeline_health (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100),
    status VARCHAR(20),
    error_message TEXT,
    checked_at TIMESTAMP DEFAULT NOW()
);

-- Agent action audit trail
CREATE TABLE IF NOT EXISTS audit_log (
    id SERIAL PRIMARY KEY,
    agent_name VARCHAR(50),
    action_taken TEXT,
    reasoning TEXT,
    success BOOLEAN,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Agent reasoning log (new)
CREATE TABLE IF NOT EXISTS reasoning_log (
    id SERIAL PRIMARY KEY,
    agent_name VARCHAR(50),
    failure_type VARCHAR(50),
    data_snapshot JSONB,
    llm_reasoning TEXT,
    decision VARCHAR(50),
    confidence VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Data quality snapshots over time (new)
CREATE TABLE IF NOT EXISTS quality_history (
    id SERIAL PRIMARY KEY,
    total_rows INTEGER,
    good_rows INTEGER,
    bad_rows INTEGER,
    duplicate_rows INTEGER,
    quality_score FLOAT,
    failure_types TEXT,
    recorded_at TIMESTAMP DEFAULT NOW()
);