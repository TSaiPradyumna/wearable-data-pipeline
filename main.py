import os
import json
import psycopg2
from fastapi import FastAPI, Request, HTTPException
from contextlib import asynccontextmanager

# 1. Database Connection Helper
def get_db_connection():
    # This looks for a hidden configuration variable named DATABASE_URL
    # which Railway will automatically provide for us later.
    return psycopg2.connect(os.getenv("DATABASE_URL"))

# 2. Automatically set up the database table when the app turns on
@asynccontextmanager
async def lifespan(app: FastAPI):
    # This code runs on startup
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
    yield
    # Any cleanup code would go here when the app shuts down

# 3. Initialize our FastAPI application
app = FastAPI(lifespan=lifespan)

# 4. The Webhook endpoint that listens for Open Wearables data
@app.post("/webhook")
async def receive_data(request: Request):
    try:
        # Open up the mail packet arriving from Open Wearables
        payload = await request.json()
        
        # Dig out the user ID and what kind of data it is (e.g., heart_rate, steps)
        user_id = payload.get("user", {}).get("user_id", "unknown_user")
        data_type = payload.get("type", "unknown_type")
        
        # Save the information directly into your PostgreSQL vault
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
        # If anything goes wrong, send back an error message
        raise HTTPException(status_code=500, detail=str(e))