const API_BASE = "http://127.0.0.1:8000";

// TIMER LOGIC for Auto-Heal
let timeLeft = 300; 

function formatTime(seconds) {
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return `${mins.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
}

function updateTimer() {
    if (timeLeft <= 0) {
        triggerHeal();
        timeLeft = 300; 
    } else {
        timeLeft--;
    }
    document.getElementById('timer').innerText = formatTime(timeLeft);
    const percent = ((300 - timeLeft) / 300) * 100;
    document.getElementById('progress-fill').style.width = percent + '%';
}

setInterval(updateTimer, 1000);

// HITL DEDICATED LOGIC
window.hitlActive = false;
let hitlInterval;

function showHitlModal() {
    window.hitlActive = true;
    document.getElementById('hitl-modal').style.display = 'block';
    
    let hitlCountdown = 15.0;
    document.getElementById('hitl-progress-fill').style.width = '100%';
    
    hitlInterval = setInterval(() => {
        hitlCountdown -= 0.1;
        document.getElementById('hitl-timer').innerText = hitlCountdown.toFixed(1);
        document.getElementById('hitl-progress-fill').style.width = ((hitlCountdown / 15.0) * 100) + '%';
        
        if (hitlCountdown <= 0) {
            clearInterval(hitlInterval);
            resolveDuplicate('keep_old'); // default
        }
    }, 100);
}

async function resolveDuplicate(action) {
    clearInterval(hitlInterval);
    document.getElementById('hitl-modal').style.display = 'none';
    
    try {
        await fetch(`${API_BASE}/resolve-duplicates`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({action: action})
        });
        alert("Duplicates resolved. Re-running pipeline.");
        setTimeout(() => {
            window.hitlActive = false;
            fetchHealth();
            fetchAuditLog();
        }, 2000);
    } catch(e) {
        console.error(e);
        window.hitlActive = false;
    }
}

// API CALLS
async function fetchHealth() {
    try {
        const res = await fetch(`${API_BASE}/health`);
        const data = await res.json();
        
        document.getElementById('total-rows').innerText = data.total_rows;
        document.getElementById('good-rows').innerText = data.good_rows;
        document.getElementById('bad-rows').innerText = data.bad_rows;
        document.getElementById('schema-ok').innerText = data.schema_ok ? "Yes" : "No";

        const statusEl = document.getElementById('health-status');
        if (data.status === "healthy" && !data.pending_duplicates) {
            statusEl.innerText = "HEALTHY";
            statusEl.className = "health-status healthy";
        } else {
            statusEl.innerText = "UNHEALTHY (Incident Detected)";
            statusEl.className = "health-status unhealthy";
        }
        
        // Trigger popup if duplicates exist and not already active
        if (data.pending_duplicates && !window.hitlActive) {
            showHitlModal();
        }

    } catch (e) {
        console.error("Health fetch failed", e);
    }
}

async function fetchAuditLog() {
    try {
        const res = await fetch(`${API_BASE}/audit`);
        const data = await res.json();
        
        const container = document.getElementById('audit-log');
        container.innerHTML = "";
        
        if (!data.entries || data.entries.length === 0) {
            container.innerHTML = "No agent actions logged yet.";
            return;
        }

        data.entries.forEach(entry => {
            const div = document.createElement('div');
            div.className = `log-entry ${entry.success ? 'success' : 'error'}`;
            div.innerHTML = `
                <div class="log-header">
                    <strong>${entry.agent_name}</strong>
                    <span>${new Date(entry.created_at).toLocaleTimeString()}</span>
                </div>
                <div style="font-weight:500; margin-bottom: 4px;">Action: ${entry.action_taken}</div>
                <div style="color: #94a3b8; font-size: 0.9em;">Reason: ${entry.reasoning.substring(0, 150)}...</div>
            `;
            container.appendChild(div);
        });
    } catch (e) {
        console.error("Audit log fetch failed", e);
    }
}

async function triggerHeal() {
    try {
        alert("Self-healing triggered. Check audit log shortly.");
        await fetch(`${API_BASE}/heal`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({dag_id: "data_ingestion_pipeline"})
        });
        setTimeout(() => {
            fetchHealth();
            fetchAuditLog();
        }, 5000);
    } catch(e) { console.error(e); }
}

async function injectFailure(type) {
    try {
        const count = parseInt(document.getElementById('inject-count').value) || 10;
        await fetch(`${API_BASE}/inject/${type}?count=${count}`, { method: 'POST' });
        alert(`Injected ${type} failure with ${count} records! The pipeline crash is processing...`);
        setTimeout(() => {
            fetchHealth();
            fetchData();
        }, 3000);
    } catch (e) { console.error(e); }
}

async function fetchData() {
    try {
        const res = await fetch(`${API_BASE}/data`);
        const data = await res.json();
        
        const tbody = document.getElementById('data-table-body');
        tbody.innerHTML = "";
        
        if (!data.data || data.data.length === 0) {
            tbody.innerHTML = "<tr><td colspan='5' style='text-align:center;'>No data available in raw_sales</td></tr>";
            return;
        }

        data.data.forEach(row => {
            const tr = document.createElement('tr');
            // Give a slight red background to bad data to make it visible!
            if (row.quantity <= 0 || row.price <= 0) {
                tr.style.backgroundColor = "rgba(239, 68, 68, 0.1)";
            }
            tr.innerHTML = `
                <td>${row.order_date.substring(0, 10)}</td>
                <td style="font-family: monospace;">${row.order_id}</td>
                <td>${row.product_id}</td>
                <td>${row.quantity}</td>
                <td>$${row.price}</td>
            `;
            tbody.appendChild(tr);
        });
    } catch (e) { console.error("Data fetch failed", e); }
}

// GUI Logics
function switchTab(tab) {
    document.querySelectorAll('.tab-content').forEach(el => el.style.display = 'none');
    document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
    
    if (tab === 'form') {
        document.getElementById('form-tab').style.display = 'block';
        document.querySelectorAll('.tab-btn')[0].classList.add('active');
    } else {
        document.getElementById('csv-tab').style.display = 'block';
        document.querySelectorAll('.tab-btn')[1].classList.add('active');
    }
}

async function sendManualData(rows) {
    try {
        const res = await fetch(`${API_BASE}/ingest-manual`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(rows)
        });
        alert(`Successfully ingested data! Dashboard updating...`);
        setTimeout(fetchHealth, 2000);
    } catch(e) { console.error(e); alert("Failed to ingest."); }
}

function submitManualData() {
    const row = {
        order_id: document.getElementById('order_id').value,
        product_id: document.getElementById('product_id').value,
        customer_id: document.getElementById('customer_id').value,
        quantity: parseInt(document.getElementById('quantity').value) || 0,
        price: parseFloat(document.getElementById('price').value) || 0,
        category: "Manual",
        region: "Manual"
    };

    if (!row.order_id || !row.product_id) return alert("Order ID and Product ID are required.");
    sendManualData([row]);
}

function uploadCsv() {
    const fileInput = document.getElementById('csv-file');
    const file = fileInput.files[0];
    if (!file) return alert("Please select a CSV file.");

    const reader = new FileReader();
    reader.onload = function(e) {
        const text = e.target.result;
        const lines = text.split(/\r?\n/);
        
        const headers = lines[0].split(',').map(h => h.trim());
        const requiredHeaders = ['order_id', 'product_id', 'customer_id', 'quantity', 'price'];
        
        // CSV Validation Engine
        for (let r of requiredHeaders) {
            if (!headers.includes(r)) {
                alert(`CSV Rejected! Missing required schema column: ${r}`);
                return;
            }
        }

        const rows = [];
        for (let i = 1; i < lines.length; i++) {
            if (!lines[i].trim()) continue;
            const values = lines[i].split(',').map(v => v.trim());
            const row = {};
            headers.forEach((h, index) => { row[h] = values[index]; });
            
            rows.push({
                order_id: row.order_id,
                product_id: row.product_id,
                customer_id: row.customer_id,
                quantity: parseInt(row.quantity) || 0,
                price: parseFloat(row.price) || 0,
                category: row.category || "CSV",
                region: row.region || "CSV"
            });
        }
        sendManualData(rows);
    };
    reader.readAsText(file);
}

// Initial loads
fetchHealth();
fetchAuditLog();
fetchData();
setInterval(fetchHealth, 5000);
setInterval(fetchAuditLog, 5000);
setInterval(fetchData, 5000);
