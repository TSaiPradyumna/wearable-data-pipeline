import os
import json
import logging
import asyncpg
from pydantic import BaseModel, Field, ValidationError
from fastapi import FastAPI, Request, HTTPException, Form
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("async_pipeline")

# Read your existing Railway database string
DATABASE_URL = os.getenv("DATABASE_URL")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Connecting to cloud PostgreSQL vault via asyncpg...")
    try:
        # Establish a temporary connection pool to handle schema verification
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS health_logs (
                id SERIAL PRIMARY KEY,
                user_id TEXT NOT NULL,
                data_type TEXT NOT NULL,
                raw_payload TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await conn.close()
        logger.info("Async PostgreSQL database migration successful.")
    except Exception as e:
        logger.critical(f"Fatal cloud storage initialization failure: {str(e)}")
    yield
    logger.info("Executing safe pipeline teardown procedures.")

app = FastAPI(title="Wearable Data Pipeline Service", version="1.0.0", lifespan=lifespan)

class TelemetryPayload(BaseModel):
    user_id: str = Field(..., min_length=3, max_length=50)
    data_type: str = Field(...)
    metric_value: int = Field(..., ge=0, le=10000)


# ==========================================
# 1. DATA INGESTION USER INTERFACE
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
            :root { --primary: #2563eb; --primary-hover: #1d4ed8; --background: #f8fafc; --surface: #ffffff; --text-main: #0f172a; --text-muted: #64748b; --border: #cbd5e1; }
            body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background-color: var(--background); color: var(--text-main); margin: 0; padding: 40px 20px; display: flex; flex-direction: column; align-items: center; }
            .portal-container { background: var(--surface); padding: 40px; border-radius: 8px; border: 1px solid #e2e8f0; box-shadow: 0 1px 3px rgba(0,0,0,0.05); width: 100%; max-width: 480px; box-sizing: border-box; }
            .brand-header { border-bottom: 2px solid #f1f5f9; padding-bottom: 20px; margin-bottom: 30px; }
            h2 { margin: 0; color: var(--text-main); font-size: 22px; font-weight: 600; letter-spacing: -0.5px; }
            .subtitle { color: var(--text-muted); font-size: 14px; margin: 8px 0 0 0; line-height: 1.5; }
            .form-group { margin-bottom: 20px; }
            label { font-weight: 500; display: block; margin-bottom: 8px; color: #334155; font-size: 13px; text-transform: uppercase; letter-spacing: 0.5px; }
            input, select { width: 100%; padding: 10px 14px; border: 1px solid var(--border); border-radius: 6px; box-sizing: border-box; font-size: 14px; color: var(--text-main); }
            input:focus, select:focus { border-color: var(--primary); outline: none; box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.15); }
            button { width: 100%; background-color: var(--primary); color: white; border: none; padding: 12px; border-radius: 6px; font-size: 14px; font-weight: 600; cursor: pointer; margin-top: 15px; }
            button:hover { background-color: var(--primary-hover); }
            .meta-navigation { margin-top: 24px; border-top: 1px solid #f1f5f9; padding-top: 20px; text-align: center; }
            .meta-navigation a { color: var(--primary); text-decoration: none; font-weight: 500; font-size: 13px; }
        </style>
    </head>
    <body>
        <div class="portal-container">
            <div class="brand-header">
                <h2>Telemetry Ingestion Gateway</h2>
                <p class="subtitle">Submit verified edge node data packets to the permanent cloud database layer using asyncpg.</p>
            </div>
            <form action="/submit-web" method="post">
                <div class="form-group">
                    <label>User Identifier (UUID)</label>
                    <input type="text" name="user_id" placeholder="e.g., usr_9921x" required>
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
                    <input type="number" name="metric_value" placeholder="e.g., 75" required>
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
        validated_data = TelemetryPayload(user_id=user_id, data_type=data_type, metric_value=metric_value)
        payload = {
            "metadata": {"source_agent": "web_gateway_portal", "compliance_version": "1.0.0"},
            "telemetry": {
                "user_id": validated_data.user_id,
                "type": validated_data.data_type,
                "metrics": {"value": validated_data.metric_value}
            }
        }
        
        # Connect asynchronously to your permanent PostgreSQL cluster
        conn = await asyncpg.connect(DATABASE_URL)
        
        # asyncpg utilizes positional parameters ($1, $2, $3) instead of %s
        await conn.execute(
            "INSERT INTO health_logs (user_id, data_type, raw_payload) VALUES ($1, $2, $3);",
            validated_data.user_id, validated_data.data_type, json.dumps(payload)
        )
        await conn.close()
        
        return HTMLResponse(content="""
            <body style="font-family:-apple-system,sans-serif; background-color:#f8fafc; text-align:center; padding:60px 20px;">
                <div style="background:#fff; border:1px solid #e2e8f0; padding:40px; border-radius:8px; display:inline-block; max-width:400px; box-shadow:0 1px 3px rgba(0,0,0,0.05);">
                    <h3 style="color:#16a34a; margin:0 0 10px 0; font-size:18px;">Transaction Confirmed</h3>
                    <p style="color:#64748b; font-size:14px; margin:0 0 25px 0; line-height:1.5;">The telemetry packet has been written permanently via asyncpg to the relational cluster database.</p>
                    <a href="/" style="background-color:#2563eb; color:white; padding:10px 20px; text-decoration:none; border-radius:5px; font-size:13px; font-weight:600;">Return to Gateway</a>
                </div>
            </body>
        """)
    except Exception as e:
        logger.error(f"Async ingestion failure: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# 3. PERMANENT SYSTEM DATA MONITOR
# ==========================================
@app.get("/history", response_class=HTMLResponse)
async def render_system_dashboard():
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        rows = await conn.fetch("SELECT id, user_id, data_type, raw_payload, created_at FROM health_logs ORDER BY id DESC;")
        await conn.close()
        
        table_rows = ""
        for row in rows:
            # asyncpg rows can be accessed like dictionaries or indexed tuples
            payload_data = json.loads(row['raw_payload'])
            metrics_string = json.dumps(payload_data.get("telemetry", {}).get("metrics", {}))
            
            table_rows += f"""
            <tr>
                <td>{row['id']}</td>
                <td><span class="user-token">{row['user_id']}</span></td>
                <td><span class="metric-type">{row['data_type']}</span></td>
                <td class="payload-cell">{metrics_string}</td>
                <td>{row['created_at']}</td>
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
                h2 {{ font-size: 20px; font-weight: 600; margin: 0; }}
                .refresh-action {{ background: #fff; color: #0f172a; border: 1px solid #cbd5e1; text-decoration: none; padding: 8px 16px; border-radius: 6px; font-weight: 500; font-size: 13px; }}
                table {{ width: 100%; background: white; border-collapse: collapse; border-radius: 8px; overflow: hidden; border: 1px solid #e2e8f0; }}
                th, td {{ padding: 14px 20px; text-align: left; font-size: 14px; border-bottom: 1px solid #f1f5f9; }}
                th {{ background-color: #f8fafc; color: #475569; font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; }}
                .user-token {{ font-family: monospace; background: #f1f5f9; color: #334155; padding: 3px 6px; border-radius: 4px; font-size: 13px; }}
                .payload-cell {{ font-family: monospace; color: #0284c7; font-size: 13px; }}
                .state-empty {{ text-align: center; color: #64748b; padding: 60px 20px; background: white; border-radius: 8px; border: 1px solid #e2e8f0; }}
            </style>
        </head>
        <body>
            <div class="dashboard-frame">
                <div class="action-bar">
                    <h2>Data Access Logging & Analytics Dashboard (Async Engine)</h2>
                    <a href="/history" class="refresh-action">Synchronize Cache Layer</a>
                </div>
                {"<table><tr><th width='8%'>ID</th><th width='25%'>Target Subject Token</th><th width='20%'>Class</th><th>Structured Metrics Node</th><th width='20%'>Ingestion Timestamp</th></tr>" + table_rows + "</table>" if rows else "<div class='state-empty'><h3>Zero Database Mutations Found</h3><p>Ingestion streams are currently vacant.</p></div>"}
            </div>
        </body>
        </html>
        """
        return HTMLResponse(content=html_content)
    except Exception as e:
        return HTMLResponse(content=f"Database Async Resolution Exception: {str(e)}", status_code=500)