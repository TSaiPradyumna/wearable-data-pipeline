import sqlite3
import json
import logging
from typing import Dict, Any, List
from pydantic import BaseModel, Field, ValidationError
from fastapi import FastAPI, Request, HTTPException, Form
from fastapi.responses import HTMLResponse, JSONResponse
from contextlib import asynccontextmanager

# Configure structured enterprise logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("production_pipeline")

DB_FILE = "health_data.db"

# Core Database Migration Management
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing storage engine state machine...")
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS health_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                data_type TEXT NOT NULL,
                raw_payload TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        conn.close()
        logger.info("Database schema verification successful. Target entities initialized.")
    except Exception as e:
        logger.critical(f"Fatal exception during storage engine migration: {str(e)}")
    yield
    logger.info("Executing safe teardown procedures. Persisting remaining cache layers.")

app = FastAPI(title="Wearable Data Pipeline Service", version="1.0.0", lifespan=lifespan)

# Enterprise Data Ingestion Validation Schemas
class TelemetryPayload(BaseModel):
    user_id: str = Field(..., min_length=3, max_length=50, examples=["patient_alpha_1"])
    data_type: str = Field(..., examples=["heart_rate", "step_count", "oxygen_saturation"])
    metric_value: int = Field(..., ge=0, le=10000, examples=[72])


# ==========================================
# 1. ENTERPRISE DESIGN INGESTION INTERFACE
# ==========================================
@app.get("/", response_class=HTMLResponse)
async def render_ingestion_portal():
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Data Ingestion Portal | Telemetry Gateway</title>
        <style>
            :root {
                --primary: #2563eb;
                --primary-hover: #1d4ed8;
                --background: #f8fafc;
                --surface: #ffffff;
                --text-main: #0f172a;
                --text-muted: #64748b;
                --border: #cbd5e1;
            }
            body { 
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; 
                background-color: var(--background); 
                color: var(--text-main);
                margin: 0; 
                padding: 40px 20px; 
                display: flex; 
                flex-direction: column; 
                align-items: center; 
            }
            .portal-container { 
                background: var(--surface); 
                padding: 40px; 
                border-radius: 8px; 
                border: 1px solid #e2e8f0;
                box-shadow: 0 1px 3px rgba(0,0,0,0.05), 0 1px 2px rgba(0,0,0,0.1); 
                width: 100%; 
                max-width: 480px; 
                box-sizing: border-box; 
            }
            .brand-header {
                border-bottom: 2px solid #f1f5f9;
                padding-bottom: 20px;
                margin-bottom: 30px;
            }
            h2 { margin: 0; color: var(--text-main); font-size: 22px; font-weight: 600; letter-spacing: -0.5px; }
            .subtitle { color: var(--text-muted); font-size: 14px; margin: 8px 0 0 0; line-height: 1.5; }
            .form-group { margin-bottom: 20px; }
            label { font-weight: 500; display: block; margin-bottom: 8px; color: #334155; font-size: 13px; text-transform: uppercase; letter-spacing: 0.5px; }
            input, select { 
                width: 100%; 
                padding: 10px 14px; 
                border: 1px solid var(--border); 
                border-radius: 6px; 
                box-sizing: border-box; 
                font-size: 14px; 
                color: var(--text-main);
                background-color: #fff;
            }
            input:focus, select:focus { 
                border-color: var(--primary); 
                outline: none; 
                box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.15); 
            }
            button { 
                width: 100%; 
                background-color: var(--primary); 
                color: white; 
                border: none; 
                padding: 12px; 
                border-radius: 6px; 
                font-size: 14px; 
                font-weight: 600; 
                cursor: pointer; 
                margin-top: 15px;
                transition: background-color 0.15s ease;
            }
            button:hover { background-color: var(--primary-hover); }
            .meta-navigation { margin-top: 24px; border-top: 1px solid #f1f5f9; padding-top: 20px; text-align: center; }
            .meta-navigation a { color: var(--primary); text-decoration: none; font-weight: 500; font-size: 13px; }
            .meta-navigation a:hover { text-decoration: underline; }
        </style>
    </head>
    <body>
        <div class="portal-container">
            <div class="brand-header">
                <h2>Telemetry Ingestion Gateway</h2>
                <p class="subtitle">Submit verified edge node data packets to the secure database storage layer.</p>
            </div>
            <form action="/submit-web" method="post">
                <div class="form-group">
                    <label>User Identifier (UUID)</label>
                    <input type="text" name="user_id" placeholder="e.g., usr_01h7abcde" required>
                </div>
                
                <div class="form-group">
                    <label>Metrics Classification</label>
                    <select name="data_type">
                        <option value="heart_rate">Heart Rate (BPM)</option>
                        <option value="step_count">Step Count (Total)</option>
                        <option value="oxygen_saturation">Peripheral Oxygen Saturation (SpO2)</option>
                    </select>
                </div>
                
                <div class="form-group">
                    <label>Quantitative Value</label>
                    <input type="number" name="metric_value" placeholder="e.g., 72" required>
                </div>
                
                <button type="submit">Execute Transmission</button>
            </form>
            <div class="meta-navigation">
                <a href="/history" target="_blank">Access Administrative Logs Data →</a>
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


# ==========================================
# 2. INGESTION DATA COMPLIANCE PIPELINE
# ==========================================
@app.post("/submit-web")
async def process_web_ingestion(user_id: str = Form(...), data_type: str = Form(...), metric_value: int = Form(...)):
    try:
        # Enforce validation using enterprise structural Pydantic layers
        validated_data = TelemetryPayload(user_id=user_id, data_type=data_type, metric_value=metric_value)
        
        payload = {
            "metadata": {"source_agent": "web_gateway_portal", "compliance_version": "1.0.0"},
            "telemetry": {
                "user_id": validated_data.user_id,
                "type": validated_data.data_type,
                "metrics": {"value": validated_data.metric_value}
            }
        }
        
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO health_logs (user_id, data_type, raw_payload) VALUES (?, ?, ?);",
            (validated_data.user_id, validated_data.data_type, json.dumps(payload))
        )
        conn.commit()
        conn.close()
        
        return HTMLResponse(content="""
            <body style="font-family:-apple-system,sans-serif; background-color:#f8fafc; text-align:center; padding:60px 20px;">
                <div style="background:#fff; border:1px solid #e2e8f0; padding:40px; border-radius:8px; display:inline-block; max-width:400px; box-shadow:0 1px 3px rgba(0,0,0,0.05);">
                    <h3 style="color:#16a34a; margin:0 0 10px 0; font-size:18px;">Transaction Confirmed</h3>
                    <p style="color:#64748b; font-size:14px; margin:0 0 25px 0; line-height:1.5;">The transaction has been successfully parsed and persisted to disk layers.</p>
                    <a href="/" style="background-color:#2563eb; color:white; padding:10px 20px; text-decoration:none; border-radius:5px; font-size:13px; font-weight:600;">Return to Gateway</a>
                </div>
            </body>
        """)
    except ValidationError as ve:
        raise HTTPException(status_code=422, detail=f"Payload Validation Rejection: {ve.errors()}")
    except Exception as e:
        logger.error(f"Ingestion pipeline write anomaly encountered: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal core execution failure.")


# ==========================================
# 3. ENTERPRISE SYSTEM DATA MONITOR
# ==========================================
@app.get("/history", response_class=HTMLResponse)
async def render_system_dashboard():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT id, user_id, data_type, raw_payload, created_at FROM health_logs ORDER BY id DESC;")
        rows = cursor.fetchall()
        conn.close()
        
        table_rows = ""
        for row in rows:
            payload_data = json.loads(row[3])
            metrics_string = json.dumps(payload_data.get("telemetry", {}).get("metrics", {}))
            
            table_rows += f"""
            <tr>
                <td>{row[0]}</td>
                <td><span class="user-token">{row[1]}</span></td>
                <td><span class="metric-type">{row[2]}</span></td>
                <td class="payload-cell">{metrics_string}</td>
                <td>{row[4]}</td>
            </tr>
            """
            
        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <title>System Logs Viewer | Administrative Control</title>
            <style>
                body {{ font-family: -apple-system, sans-serif; background-color: #f8fafc; margin: 0; padding: 40px 20px; color: #0f172a; }}
                .dashboard-frame {{ max-width: 1100px; margin: 0 auto; }}
                .action-bar {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 30px; border-bottom: 2px solid #edf2f7; padding-bottom: 15px; }}
                h2 {{ font-size: 20px; font-weight: 600; margin: 0; letter-spacing: -0.5px; }}
                .refresh-action {{ background: #fff; color: #0f172a; border: 1px solid #cbd5e1; text-decoration: none; padding: 8px 16px; border-radius: 6px; font-weight: 500; font-size: 13px; transition: background 0.1s; }}
                .refresh-action:hover {{ background: #f8fafc; }}
                table {{ width: 100%; background: white; border-collapse: collapse; border-radius: 8px; overflow: hidden; border: 1px solid #e2e8f0; }}
                th, td {{ padding: 14px 20px; text-align: left; font-size: 14px; border-bottom: 1px solid #f1f5f9; }}
                th {{ background-color: #f8fafc; color: #475569; font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; }}
                tr:hover {{ background-color: #f8fafc; }}
                .user-token {{ font-family: monospace; background: #f1f5f9; color: #334155; padding: 3px 6px; border-radius: 4px; font-size: 13px; }}
                .metric-type {{ color: #475569; font-weight: 500; }}
                .payload-cell {{ font-family: monospace; color: #0284c7; font-size: 13px; }}
                .state-empty {{ text-align: center; color: #64748b; padding: 60px 20px; background: white; border-radius: 8px; border: 1px solid #e2e8f0; }}
            </style>
        </head>
        <body>
            <div class="dashboard-frame">
                <div class="action-bar">
                    <h2>Data Access Logging & Analytics Dashboard</h2>
                    <a href="/history" class="refresh-action">Synchronize Cache Layer</a>
                </div>
                {"<table><tr><th width='8%'>ID</th><th width='25%'>Target Subject Token</th><th width='20%'>Class</th><th>Structured Metrics Node</th><th width='20%'>Ingestion Timestamp</th></tr>" + table_rows + "</table>" if rows else "<div class='state-empty'><h3>Zero Database Mutations Found</h3><p>Ingestion streams are currently vacant.</p></div>"}
            </div>
        </body>
        </html>
        """
        return HTMLResponse(content=html_content)
    except Exception as e:
        logger.error(f"Dashboard view resolution exception: {str(e)}")
        return HTMLResponse(content="Internal Server Error", status_code=500)


# ==========================================
# 4. JSON REST API FOR DIRECT EDGE COMPILING
# ==========================================
@app.post("/webhook")
async def receive_edge_telemetry(payload: TelemetryPayload):
    try:
        db_payload = {
            "metadata": {"source_agent": "edge_device_firmware", "compliance_version": "1.0.0"},
            "telemetry": {
                "user_id": payload.user_id,
                "type": payload.data_type,
                "metrics": {"value": payload.metric_value}
            }
        }
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO health_logs (user_id, data_type, raw_payload) VALUES (?, ?, ?);",
            (payload.user_id, payload.data_type, json.dumps(db_payload))
        )
        conn.commit()
        conn.close()
        return JSONResponse(status_code=201, content={"status": "success", "message": "Telemetry committed."})
    except Exception as e:
        logger.error(f"REST client terminal serialization anomaly: {str(e)}")
        return JSONResponse(status_code=500, content={"status": "error", "message": "Persistence fault."})