import os
import json
from fastapi import FastAPI, Request, HTTPException
from contextlib import asynccontextmanager

# 1. BULLETPROOF DATABASE DRIVER IMPORT
try:
    # Try importing standard psycopg2
    import psycopg2
except ImportError:
    # Fallback: Force the binary version to masquerade as standard psycopg2 globally
    from psycopg2 import _psycopg as _
    import psycopg2_binary as psycopg2

# 2. Database Connection Helper
def get_db_connection():
    # Now 'psycopg2' is guaranteed to be defined globally for this function!
    return psycopg2.connect(os.getenv("DATABASE_URL"))

# 3. Automated Table Creator (Runs on Application Startup)
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Application booting up... Connecting to database vault.")
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
        conn.close()
        print("Database table verification complete: 'health_logs' table is ready.")
    except Exception as e:
        print(f"CRITICAL ERROR during startup database configuration: {str(e)}")
    
    yield
    print("Application shutting down smoothly.")

# 4. Initialize FastAPI
app = FastAPI(lifespan=lifespan)

# 5. The Webhook Listener Endpoint
@app.post("/webhook")
async def receive_data(request: Request):
    try:
        payload = await request.json()
        
        user_id = payload.get("user", {}).get("user_id", "unknown_user")
        data_type = payload.get("type", "unknown_type")
        
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO health_logs (user_id, data_type, raw_payload) VALUES (%s, %s, %s);",
            (user_id, data_type, json.dumps(payload))
        )
        conn.commit()
        cur.close()
        conn.close()
        
        return {"status": "success", "message": "Saved to database!"}
        
    except Exception as e:
        print(f"Webhook Interception Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database Write Failure: {str(e)}")