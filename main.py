import os
import json
import logging
import asyncpg
from pydantic import BaseModel, Field
from fastapi import FastAPI, Request, HTTPException, Form, Header
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templatetypes import Jinja2Templates
from contextlib import asynccontextmanager
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("clinical_pipeline")

DATABASE_URL = os.getenv("DATABASE_URL")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Verifying multi-table structural database models...")
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        
        # Table 1: Core Diagnostics Vitals Log
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
        
        # Table 2: Specialized Geospatial Location & Steps Telemetry Log
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS patient_movement (
                id SERIAL PRIMARY KEY,
                user_id TEXT NOT NULL,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                steps INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        await conn.close()
        logger.info("All relational cloud database tables verified: ONLINE.")
    except Exception as e:
        logger.critical(f"Fatal infrastructure boot configuration error: {str(e)}")
    yield
    logger.info("Safe pipeline shutdown sequence complete.")

app = FastAPI(title="Clinical Telemetry Pipeline Gateway", version="4.3.0", lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

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

LABELS_MATRIX = {
    "heart_rate": "Heart Rate (BPM)",
    "oxygen_saturation": "Pulse Oximetry (SpO2 %)",
    "blood_glucose": "Blood Glucose (mg/dL)",
    "systolic_bp": "Systolic Blood Pressure (mmHg)",
    "respiratory_rate": "Respiratory Rate (RPM)",
    "body_temperature": "Core Body Temperature (°F)"
}

# ==========================================
# ENTERPRISE INTERFACE ROUTER SYSTEMS
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
        return templates.TemplateResponse("history.html", {"request": request, "rows": rows, "labels": LABELS_MATRIX})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/submit-ticket")
async def process_support_ticket(email: str = Form(...), severity: str = Form(...), message: str = Form(...)):
    return HTMLResponse(content="<h3>Technical Case Escalated Logged</h3>")

# DUAL-MODE LOG INGESTION
@app.post("/submit-web")
async def process_web_ingestion(
    user_id_form: Optional[str] = Form(None, alias="user_id"), 
    patient_name_form: Optional[str] = Form(None, alias="patient_name"), 
    patient_age_form: Optional[int] = Form(None, alias="patient_age"), 
    ward_number_form: Optional[str] = Form(None, alias="ward_number"), 
    data_type_form: Optional[str] = Form(None, alias="data_type"), 
    metric_value_form: Optional[int] = Form(None, alias="metric_value"),
    user_id_head: Optional[str] = Header(None, alias="user_id"), 
    patient_name_head: Optional[str] = Header(None, alias="patient_name"), 
    patient_age_head: Optional[int] = Header(None, alias="patient_age"), 
    ward_number_head: Optional[str] = Header(None, alias="ward_number"), 
    data_type_head: Optional[str] = Header(None, alias="data_type"), 
    metric_value_head: Optional[int] = Header(None, alias="metric_value")
):
    try:
        user_id = user_id_form or user_id_head
        patient_name = patient_name_form or patient_name_head
        patient_age = patient_age_form or patient_age_head
        ward_number = ward_number_form or ward_number_head
        data_type = data_type_form or data_type_head
        metric_value = metric_value_form or metric_value_head

        if not user_id:
            raise HTTPException(status_code=422, detail="Missing baseline identity context keys.")

        validated = ClinicalPayload(user_id=str(user_id), patient_name=str(patient_name), patient_age=int(patient_age), ward_number=str(ward_number), data_type=str(data_type), metric_value=int(metric_value))
        acuity = evaluate_clinical_acuity(validated.data_type, validated.metric_value)
        payload = {"system_headers": {"pipeline_agent": "dual_mode_v4_3"}, "clinical_node": {"patient_id": validated.user_id, "observed_value": validated.metric_value}}
        
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute(
            """INSERT INTO health_logs (user_id, patient_name, patient_age, ward_number, data_type, metric_value, acuity_status, raw_payload) 
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8);""",
            validated.user_id, validated.patient_name, validated.patient_age, validated.ward_number, validated.data_type, validated.metric_value, acuity, json.dumps(payload)
        )
        await conn.close()
        return HTMLResponse(content="<h3>Diagnostic Package Saved</h3>")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==========================================
# NEW BIOMOBILITY PIPELINE LAYER (FORM & HEADERS)
# ==========================================

@app.get("/movement", response_class=HTMLResponse)
async def render_mobility_dashboard(request: Request):
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        rows = await conn.fetch("SELECT id, user_id, latitude, longitude, steps, created_at FROM patient_movement ORDER BY id DESC LIMIT 50;")
        await conn.close()
        return templates.TemplateResponse("movement.html", {"request": request, "rows": rows})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/submit-movement")
async def process_spatial_telemetry(
    user_id_head: Optional[str] = Header(None, alias="user_id"),
    latitude_head: Optional[float] = Header(None, alias="latitude"),
    longitude_head: Optional[float] = Header(None, alias="longitude"),
    steps_head: Optional[int] = Header(None, alias="steps"),
    user_id_form: Optional[str] = Form(None, alias="user_id"),
    latitude_form: Optional[float] = Form(None, alias="latitude"),
    longitude_form: Optional[float] = Form(None, alias="longitude"),
    steps_form: Optional[int] = Form(None, alias="steps")
):
    try:
        user_id = user_id_head or user_id_form
        latitude = latitude_head or latitude_form
        longitude = longitude_head or longitude_form
        steps = steps_head or steps_form

        if not user_id or latitude is None or longitude is None or steps is None:
            raise HTTPException(status_code=422, detail="Incomplete tracking hardware context headers/fields.")

        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute(
            """INSERT INTO patient_movement (user_id, latitude, longitude, steps) 
               VALUES ($1, $2, $3, $4);""",
            str(user_id), float(latitude), float(longitude), int(steps)
        )
        await conn.close()
        return {"status": "SUCCESS", "message": "Spatial coordinate logged."}
    except Exception as e:
        logger.error(f"Mobility Ingestion Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))