import os
import json
import logging
import asyncpg
from pydantic import BaseModel, Field
from fastapi import FastAPI, Request, HTTPException, Form, Header
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager
from typing import Optional

# Configure structured enterprise telemetry logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("clinical_pipeline")

DATABASE_URL = os.getenv("DATABASE_URL")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing clinical storage database configurations...")
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        # Verify and apply relational schema migrations seamlessly
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
        logger.info("Clinical database connection status: ACTIVE.")
    except Exception as e:
        logger.critical(f"Fatal storage verification exception: {str(e)}")
    yield
    logger.info("Executing safe pipeline teardown procedures.")

app = FastAPI(title="Clinical Telemetry Pipeline Gateway", version="4.2.0", lifespan=lifespan)

# Define static HTML template system directory location
templates = Jinja2Templates(directory="templates")

# Strict Type Validation Schema
class ClinicalPayload(BaseModel):
    user_id: str = Field(..., min_length=3, max_length=50)
    patient_name: str = Field(..., min_length=2, max_length=100)
    patient_age: int = Field(..., ge=0, le=125)
    ward_number: str = Field(..., min_length=1, max_length=20)
    data_type: str = Field(...)
    metric_value: int = Field(..., ge=0, le=5000)

# Physiological Medical Triage Engine (Modified Early Warning Score Variant)
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
        # Internal database format uses integer-scaled decimals (e.g. 98.6 is passed as 986)
        return "CRITICAL" if value < 950 or value > 1030 else "MONITOR" if value < 970 or value > 995 else "NORMAL"
    return "NORMAL"

LABELS_MATRIX = {
    "heart_rate": "Heart Rate (BPM)",
    "oxygen_saturation": "Pulse Oximetry (SpO2 %)",
    "blood_glucose": "Blood Glucose (mg/dL)",
    "systolic_bp": "Systolic Blood Pressure (mmHg)",
    "respiratory_rate": "Respiratory Rate (RPM)",
    "body_temperature": "Core Body Temperature (°F)"
}

# ==========================================
# CORE APPLICATION ROUTING INFRASTRUCTURE
# ==========================================

@app.get("/", response_class=HTMLResponse)
async def render_home_portal(request: Request):
    return templates.TemplateResponse("home.html", {"request": request})

@app.get("/about", response_class=HTMLResponse)
async def render_about_page(request: Request):
    return templates.TemplateResponse("about.html", {"request": request})

@app.get("/contact", response_class=HTMLResponse)
async def render_contact_page(request: Request):
    return templates.TemplateResponse("contact.html", {"request": request})

@app.get("/history", response_class=HTMLResponse)
async def render_system_dashboard(request: Request):
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        rows = await conn.fetch("SELECT id, user_id, patient_name, patient_age, ward_number, data_type, metric_value, acuity_status, created_at FROM health_logs ORDER BY id DESC;")
        await conn.close()
        
        return templates.TemplateResponse("history.html", {
            "request": request, 
            "rows": rows, 
            "labels": LABELS_MATRIX
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/submit-ticket")
async def process_support_ticket(email: str = Form(...), severity: str = Form(...), message: str = Form(...)):
    logger.info(f"Escalation ticket registered from system node: {email} [{severity.upper()}]")
    return HTMLResponse(content=f"""
        <body style="font-family:-apple-system,sans-serif; background-color:#f1f5f9; text-align:center; padding:60px 20px;">
            <div style="background:#fff; border:1px solid #e2e8f0; padding:40px; border-radius:6px; display:inline-block; max-width:420px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05);">
                <h3 style="color:#0284c7; margin:0 0 10px 0;">Ticket Transmitted</h3>
                <p style="color:#475569; font-size:14px; margin-bottom:20px;">Support case route confirmed. Dispatch identification code log generated.</p>
                <a href="/contact" style="color:#0284c7; font-weight:600; text-decoration:none;">← Return to Support Portal</a>
            </div>
        </body>
    """)

# DUAL-MODE HIGH ACCESSIBILITY INGESTION INTERFACE
@app.post("/submit-web")
async def process_web_ingestion(
    # Mode 1: Extraction Parameters from Manual Browser Forms
    user_id_form: Optional[str] = Form(None, alias="user_id"), 
    patient_name_form: Optional[str] = Form(None, alias="patient_name"), 
    patient_age_form: Optional[int] = Form(None, alias="patient_age"), 
    ward_number_form: Optional[str] = Form(None, alias="ward_number"), 
    data_type_form: Optional[str] = Form(None, alias="data_type"), 
    metric_value_form: Optional[int] = Form(None, alias="metric_value"),
    
    # Mode 2: Extraction Parameters from Hardware Device Header Fields (MacroDroid)
    user_id_head: Optional[str] = Header(None, alias="user_id"), 
    patient_name_head: Optional[str] = Header(None, alias="patient_name"), 
    patient_age_head: Optional[int] = Header(None, alias="patient_age"), 
    ward_number_head: Optional[str] = Header(None, alias="ward_number"), 
    data_type_head: Optional[str] = Header(None, alias="data_type"), 
    metric_value_head: Optional[int] = Header(None, alias="metric_value")
):
    try:
        # Cross-reference incoming structures to prioritize Form elements, falling back to Headers
        user_id = user_id_form or user_id_head
        patient_name = patient_name_form or patient_name_head
        patient_age = patient_age_form or patient_age_head
        ward_number = ward_number_form or ward_number_head
        data_type = data_type_form or data_type_head
        metric_value = metric_value_form or metric_value_head

        # Enforce validation failure if parameters are absent across both layers
        if not user_id:
            raise HTTPException(status_code=422, detail="Payload properties unassigned across processing variants.")

        # Consolidate parsed data into strict verification layer
        validated = ClinicalPayload(
            user_id=str(user_id), 
            patient_name=str(patient_name), 
            patient_age=int(patient_age),
            ward_number=str(ward_number), 
            data_type=str(data_type), 
            metric_value=int(metric_value)
        )
        
        acuity = evaluate_clinical_acuity(validated.data_type, validated.metric_value)
        payload = {
            "system_headers": {"pipeline_agent": "dual_mode_v4_2"},
            "clinical_node": {"patient_id": validated.user_id, "observed_value": validated.metric_value}
        }
        
        # Commit to PostgreSQL asynchronously
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute(
            """INSERT INTO health_logs (user_id, patient_name, patient_age, ward_number, data_type, metric_value, acuity_status, raw_payload) 
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8);""",
            validated.user_id, validated.patient_name, validated.patient_age, validated.ward_number,
            validated.data_type, validated.metric_value, acuity, json.dumps(payload)
        )
        await conn.close()
        
        return HTMLResponse(content="""
            <body style="font-family:sans-serif; background-color:#f1f5f9; text-align:center; padding:60px 20px;">
                <div style="background:#fff; border:1px solid #e2e8f0; padding:40px; border-radius:6px; display:inline-block; max-width:420px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05);">
                    <h3 style="color:#16a34a; margin:0 0 10px 0;">Record Composed</h3>
                    <p style="color:#475569; font-size:14px; margin-bottom:20px;">Telemetry metrics written permanently to cloud database vault tier.</p>
                    <a href="/" style="color:#0284c7; font-weight:600; text-decoration:none;">← Continue Ingestion Monitor</a>
                </div>
            </body>
        """)
    except Exception as e:
        logger.error(f"Pipeline Intake Routing Abort: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))