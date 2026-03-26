CREATE TABLE IF NOT EXISTS raw_sales (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(20),
    quantity INTEGER,
    price FLOAT,
    sale_date DATE,
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_health (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100),
    status VARCHAR(20),
    error_message TEXT,
    checked_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_log (
    id SERIAL PRIMARY KEY,
    agent_name VARCHAR(50),
    action_taken TEXT,
    reasoning TEXT,
    success BOOLEAN,
    created_at TIMESTAMP DEFAULT NOW()
);