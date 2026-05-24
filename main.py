import sqlite3
import json
from fastapi import FastAPI, Request, HTTPException
from contextlib import asynccontextmanager

DB_FILE = "health_data.db"

# 1. Database Automated Table Creator (Runs on Startup)
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Application booting up... Initializing SQLite storage engine.")
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        # Create a table to hold your fitness data packets
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS health_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                data_type TEXT,
                raw_payload TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        conn.close()
        print("Storage engine configuration complete: 'health_logs' is ready.")
    except Exception as e:
        print(f"CRITICAL STARTUP ERROR: {str(e)}")
    yield
    print("Application shutting down smoothly.")

# 2. Initialize FastAPI
app = FastAPI(lifespan=lifespan)

# 3. The Webhook Listener Endpoint
@app.post("/webhook")
async def receive_data(request: Request):
    try:
        payload = await request.json()
        
        user_id = payload.get("user", {}).get("user_id", "unknown_user")
        data_type = payload.get("type", "unknown_type")
        
        # Connect and save directly to the local disk file
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO health_logs (user_id, data_type, raw_payload) VALUES (?, ?, ?);",
            (user_id, data_type, json.dumps(payload))
        )
        conn.commit()
        conn.close()
        
        return {"status": "success", "message": "Saved to local cloud storage vault!"}
        
    except Exception as e:
        print(f"Webhook Interception Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database Write Failure: {str(e)}")