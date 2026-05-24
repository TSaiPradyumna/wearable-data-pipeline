import os
import json
import logging
import asyncpg
from pydantic import BaseModel, Field
from fastapi import FastAPI, Request, HTTPException, Form
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("clinical_pipeline")

DATABASE_URL = os.getenv("DATABASE_URL")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Executing database migration with expanded clinical parameters...")
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        
        # Drops the old out-of-date table structure so PostgreSQL can apply the new schemas
        await conn.execute("DROP TABLE IF EXISTS health_logs;")
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS health_logs (
                id SERIAL PRIMARY KEY,
                user_id TEXT NOT NULL,
                patient_name TEXT NOT NULL,
                patient_age INTEGER NOT NULL,
                ward_number TEXT NOT NULL,
                data_type TEXT NOT NULL,
                metric_value INTEGER NOT NULL,
                acuity_status TEXT NOT NULL,
                raw_payload TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await conn.close()
        logger.info("Clinical database engine synchronized successfully.")
    except Exception as e:
        logger.critical(f"Fatal initialization failure on cloud cluster storage: {str(e)}")
    yield

app = FastAPI(title="Clinical Telemetry Pipeline Gateway", version="3.0.0", lifespan=lifespan)

# Enterprise Validation Model for Patient Demographics and Vitals
class ClinicalPayload(BaseModel):
    user_id: str = Field(..., min_length=3, max_length=50)
    patient_name: str = Field(..., min_length=2, max_length=100)
    patient_age: int = Field(..., ge=0, le=125)
    ward_number: str = Field(..., min_length=1, max_length=20)
    data_type: str = Field(...)
    metric_value: int = Field(..., ge=0, le=5000)

def evaluate_clinical_acuity(metric: str, value: int) -> str:
    if metric == "heart_rate":
        return "CRITICAL" if value < 40 or value > 130 else "MONITOR" if value < 60 or value > 100 else "NORMAL"
    elif metric == "oxygen_saturation":
        return "CRITICAL" if value < 90 else "MONITOR" if value < 95 else "NORMAL"
    elif metric == "blood_glucose":
        return "CRITICAL" if value < 60 or value > 250 else "MONITOR" if value < 70 or value > 140 else "NORMAL"
    elif metric == "systolic_bp":
        return "CRITICAL" if value < 80 or value > 180 else "MONITOR" if value < 90 or value > 130 else "NORMAL"
    elif metric == "respiratory_rate":
        return "CRITICAL" if value < 8 or value > 30 else "MONITOR" if value < 12 or value > 20 else "NORMAL"
    elif metric == "body_temperature":
        return "CRITICAL" if value < 950 or value > 1030 else "MONITOR" if value < 970 or value > 995 else "NORMAL"
    return "NORMAL"

def get_metric_label(metric: str) -> str:
    labels = {
        "heart_rate": "Heart Rate (BPM)",
        "oxygen_saturation": "Pulse Oximetry (SpO2 %)",
        "blood_glucose": "Blood Glucose (mg/dL)",
        "systolic_bp": "Systolic Blood Pressure (mmHg)",
        "respiratory_rate": "Respiratory Rate (RPM)",
        "body_temperature": "Core Body Temperature (°F x10)"
    }
    return labels.get(metric, metric)

# ==========================================
# 1. PROFESSIONAL HOSPITAL ADMISSION FORM
# ==========================================
@app.get("/", response_class=HTMLResponse)
async def render_ingestion_portal():
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Clinical Data Ingestion | Care Network Gateway</title>
        <style>
            :root { --primary: #0284c7; --primary-hover: #0369a1; --background: #f1f5f9; --surface: #ffffff; --text-main: #0f172a; --text-muted: #475569; --border: #cbd5e1; }
            body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background-color: var(--background); color: var(--text-main); margin: 0; padding: 40px 20px; display: flex; flex-direction: column; align-items: center; }
            .portal-container { background: var(--surface); padding: 40px; border-radius: 6px; border: 1px solid #e2e8f0; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05); width: 100%; max-width: 550px; box-sizing: border-box; }
            .brand-header { border-bottom: 2px solid #e2e8f0; padding-bottom: 20px; margin-bottom: 25px; }
            h2 { margin: 0; color: var(--text-main); font-size: 20px; font-weight: 600; letter-spacing: -0.3px; }
            .subtitle { color: var(--text-muted); font-size: 13px; margin: 6px 0 0 0; line-height: 1.5; }
            .form-row { display: flex; gap: 15px; margin-bottom: 18px; }
            .form-group { flex: 1; min-width: 0; }
            .form-group.full { margin-bottom: 18px; }
            label { font-weight: 600; display: block; margin-bottom: 6px; color: #334155; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; }
            input, select { width: 100%; padding: 10px 12px; border: 1px solid var(--border); border-radius: 4px; box-sizing: border-box; font-size: 14px; color: var(--text-main); background-color: #fff; }
            input:focus, select:focus { border-color: var(--primary); outline: none; box-shadow: 0 0 0 3px rgba(2, 132, 199, 0.15); }
            button { width: 100%; background-color: var(--primary); color: white; border: none; padding: 12px; border-radius: 4px; font-size: 14px; font-weight: 600; cursor: pointer; margin-top: 10px; transition: background 0.15s; }
            button:hover { background-color: var(--primary-hover); }
            .meta-navigation { margin-top: 24px; border-top: 1px solid #e2e8f0; padding-top: 20px; text-align: center; }
            .meta-navigation a { color: var(--primary); text-decoration: none; font-weight: 500; font-size: 13px; }
        </style>
    </head>
    <body>
        <div class="portal-container">
            <div class="brand-header">
                <h2>Clinical Ingestion Gateway</h2>
                <p class="subtitle">Secure electronic medical interface for recording critical patient observations directly to the relational central database server.</p>
            </div>
            <form action="/submit-web" method="post">
                <div class="form-row">
                    <div class="form-group">
                        <label>Patient Encounter ID</label>
                        <input type="text" name="user_id" placeholder="e.g., PT-901" required>
                    </div>
                    <div class="form-group">
                        <label>Assigned Ward / Room</label>
                        <input type="text" name="ward_number" placeholder="e.g., Ward 4B-02" required>
                    </div>
                </div>

                <div class="form-row">
                    <div class="form-group">
                        <label>Patient Full Name</label>
                        <input type="text" name="patient_name" placeholder="e.g., John Doe" required>
                    </div>
                    <div class="form-group" style="flex: 0 0 100px;">
                        <label>Age</label>
                        <input type="number" name="patient_age" placeholder="Age" required>
                    </div>
                </div>
                
                <div class="form-group full">
                    <label>Physiological Metric Classification</label>
                    <select name="data_type">
                        <option value="heart_rate">Heart Rate (BPM)</option>
                        <option value="oxygen_saturation">Pulse Oximetry (SpO2 %)</option>
                        <option value="blood_glucose">Blood Glucose (mg/dL)</option>
                        <option value="systolic_bp">Systolic Blood Pressure (mmHg)</option>
                        <option value="respiratory_rate">Respiratory Rate (RPM)</option>
                        <option value="body_temperature">Core Body Temp (Fahrenheit x10, e.g. 986)</option>
                    </select>
                </div>
                
                <div class="form-group full">
                    <label>Observed Value</label>
                    <input type="number" name="metric_value" placeholder="Enter numerical metric reading" required>
                </div>
                
                <button type="submit">Commit Clinical Log</button>
            </form>
            <div class="meta-navigation">
                <a href="/history">Access Central Surveillance Dashboard →</a>
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

# ==========================================
# 2. DATA PROCESSING & TRIAGE PIPELINE
# ==========================================
@app.post("/submit-web")
async def process_web_ingestion(
    user_id: str = Form(...), 
    patient_name: str = Form(...), 
    patient_age: int = Form(...), 
    ward_number: str = Form(...), 
    data_type: str = Form(...), 
    metric_value: int = Form(...)
):
    try:
        validated = ClinicalPayload(
            user_id=user_id, patient_name=patient_name, patient_age=patient_age,
            ward_number=ward_number, data_type=data_type, metric_value=metric_value
        )
        acuity = evaluate_clinical_acuity(validated.data_type, validated.metric_value)
        
        payload = {
            "system_headers": {"pipeline_agent": "hosp_node_v3", "triage_engine": "mews_v1.1.0"},
            "clinical_node": {
                "patient_id": validated.user_id,
                "name": validated.patient_name,
                "age": validated.patient_age,
                "ward": validated.ward_number,
                "metric_class": validated.data_type,
                "observed_value": validated.metric_value,
                "calculated_acuity": acuity
            }
        }
        
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute(
            """INSERT INTO health_logs (user_id, patient_name, patient_age, ward_number, data_type, metric_value, acuity_status, raw_payload) 
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8);""",
            validated.user_id, validated.patient_name, validated.patient_age, validated.ward_number,
            validated.data_type, validated.metric_value, acuity, json.dumps(payload)
        )
        await conn.close()
        
        return HTMLResponse(content=f"""
            <body style="font-family:-apple-system,sans-serif; background-color:#f1f5f9; text-align:center; padding:60px 20px;">
                <div style="background:#fff; border:1px solid #e2e8f0; padding:40px; border-radius:6px; display:inline-block; max-width:420px; box-shadow:0 4px 6px -1px rgba(0,0,0,0.05);">
                    <h3 style="color:#0284c7; margin:0 0 10px 0; font-size:18px;">Log Transmitted Successfully</h3>
                    <p style="color:#475569; font-size:14px; margin:0 0 25px 0; line-height:1.5;">Patient: <strong>{validated.patient_name}</strong> has been processed to database. Triage Status: <strong>{acuity}</strong></p>
                    <a href="/" style="background-color:#0284c7; color:white; padding:10px 20px; text-decoration:none; border-radius:4px; font-size:13px; font-weight:600;">Return to Gateway</a>
                </div>
            </body>
        """)
    except Exception as e:
        logger.error(f"Ingestion anomaly caught: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ==========================================
# 3. HIGH-ACUITY CLINICAL MONITOR DASHBOARD
# ==========================================
@app.get("/history", response_class=HTMLResponse)
async def render_system_dashboard():
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        rows = await conn.fetch("SELECT id, user_id, patient_name, patient_age, ward_number, data_type, metric_value, acuity_status, created_at FROM health_logs ORDER BY id DESC;")
        await conn.close()
        
        table_rows = ""
        for row in rows:
            acuity = row['acuity_status']
            badge_color, text_color = ("#fee2e2", "#991b1b") if acuity == "CRITICAL" else ("#fef3c7", "#92400e") if acuity == "MONITOR" else ("#dcfce7", "#166534")
            
            display_value = row['metric_value']
            if row['data_type'] == "body_temperature":
                display_value = f"{display_value / 10:.1f} °F"
                
            table_rows += f"""
            <tr>
                <td>{row['id']}</td>
                <td><span class="patient-id-token">{row['user_id']}</span></td>
                <td><strong>{row['patient_name']}</strong> <span style="color:#64748b; font-size:12px;">({row['patient_age']}yo)</span></td>
                <td><span class="ward-tag">{row['ward_number']}</span></td>
                <td>{get_metric_label(row['data_type'])}</td>
                <td style="font-weight: 600; font-size:15px; color:#0f172a;">{display_value}</td>
                <td><span class="acuity-badge" style="background-color: {badge_color}; color: {text_color};">{acuity}</span></td>
                <td style="color:#64748b; font-size:12px;">{row['created_at'].strftime('%Y-%m-%d %H:%M:%S')}</td>
            </tr>
            """
            
        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <title>Central Telemetry Surveillance View</title>
            <style>
                body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; background-color: #f8fafc; margin: 0; padding: 40px; color: #0f172a; }}
                .container-frame {{ max-width: 1300px; margin: 0 auto; }}
                .action-bar {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 25px; border-bottom: 2px solid #e2e8f0; padding-bottom: 15px; }}
                h2 {{ font-size: 20px; font-weight: 600; margin: 0; color: #1e3a8a; }}
                .refresh-action {{ background: #ffffff; color: #0f172a; border: 1px solid #cbd5e1; text-decoration: none; padding: 8px 16px; border-radius: 4px; font-weight: 500; font-size: 13px; }}
                table {{ width: 100%; background: white; border-collapse: collapse; border-radius: 6px; overflow: hidden; border: 1px solid #e2e8f0; }}
                th, td {{ padding: 14px 18px; text-align: left; font-size: 14px; border-bottom: 1px solid #f1f5f9; }}
                th {{ background-color: #f8fafc; color: #475569; font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; border-bottom: 2px solid #e2e8f0; }}
                tr:hover {{ background-color: #fafafa; }}
                .patient-id-token {{ font-family: monospace; background: #e2e8f0; color: #1e293b; padding: 4px 6px; border-radius: 4px; font-weight: 600; font-size: 12px; }}
                .ward-tag {{ background: #f1f5f9; color: #475569; padding: 3px 6px; border-radius: 4px; font-size: 13px; }}
                .acuity-badge {{ padding: 4px 10px; border-radius: 12px; font-weight: 700; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; display: inline-block; }}
                .state-empty {{ text-align: center; color: #64748b; padding: 60px 20px; background: white; border-radius: 6px; border: 1px solid #e2e8f0; }}
            </style>
        </head>
        <body>
            <div class="container-frame">
                <div class="action-bar">
                    <h2>Central Telemetry Surveillance Station</h2>
                    <a href="/history" class="refresh-action">Force Sync Telemetry Layer</a>
                </div>
                {"<table><tr><th width='6%'>ID</th><th width='15%'>Encounter ID</th><th width='22%'>Patient Context</th><th width='15%'>Ward/Room</th><th width='20%'>Metric</th><th width='12%'>Value</th><th width='12%'>Triage</th><th>Timestamp</th></tr>" + table_rows + "</table>" if rows else "<div class='state-empty'><h3>No Active Patient Surveillance Feeds</h3><p>Awaiting ingestion streams from active medical hardware.</p></div>"}
            </div>
        </body>
        </html>
        """
        return HTMLResponse(content=html_content)
    except Exception as e:
        return HTMLResponse(content=f"Surveillance Engine Error: {str(e)}", status_code=500)