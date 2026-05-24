import os
import json
import logging
from fastapi import FastAPI, Request, HTTPException
from contextlib import asynccontextmanager

try:
    import psycopg2
    from psycopg2.extras import Json
except Exception as exc:
    raise RuntimeError("Missing dependency 'psycopg2'. Install with: pip install psycopg2-binary") from exc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_db_connection():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL environment variable is not set")
    return psycopg2.connect(dsn)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application booting up... Connecting to database.")
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS health_logs (
                id SERIAL PRIMARY KEY,
                user_id TEXT,
                data_type TEXT,
                raw_payload JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        cur.close()
        logger.info("Database table verification complete: 'health_logs' table is ready.")
    except Exception:
        logger.exception("CRITICAL ERROR during startup database configuration")
        raise
    finally:
        if conn:
            conn.close()

    try:
        yield
    finally:
        logger.info("Application shutting down smoothly.")


app = FastAPI(lifespan=lifespan)


@app.post("/webhook")
async def receive_data(request: Request):
    try:
        payload = await request.json()
    except Exception:
        logger.exception("Invalid JSON payload received")
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    user_id = payload.get("user", {}).get("user_id", "unknown_user")
    data_type = payload.get("type", "unknown_type")

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO health_logs (user_id, data_type, raw_payload) VALUES (%s, %s, %s);",
            (user_id, data_type, Json(payload)),
        )
        conn.commit()
        cur.close()
        return {"status": "success", "message": "Saved to database!"}
    except Exception:
        logger.exception("Webhook Interception Error")
        raise HTTPException(status_code=500, detail="Database Write Failure")
    finally:
        if conn:
            conn.close()