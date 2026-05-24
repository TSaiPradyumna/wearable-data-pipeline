import os
import json
import psycopg2
from fastapi import FastAPI, Request, HTTPException
from contextlib import asynccontextmanager

# 1. Database Connection Helper
def get_db_connection():
    # Looks for your hidden Railway DATABASE_URL configuration variable
    return psycopg2.connect(os.getenv("DATABASE_URL"))

# 2. Automated Table Creator (Runs on Application Startup)
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Application booting up... Connecting to database vault.")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # Automatically creates the database spreadsheet table if it doesn't exist
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

# 3. Initialize FastAPI with our background startup manager
app = FastAPI(lifespan=lifespan)

# 4. The Webhook Listener Endpoint
@app.post("/webhook")
async def receive_data(request: Request):
    try:
        # Open up the incoming fitness mail packet arriving from the internet
        payload = await request.json()
        
        # Dig out who sent it and what kind of data it represents
        user_id = payload.get("user", {}).get("user_id", "unknown_user")
        data_type = payload.get("type", "unknown_type")
        
        # Securely write the data inside your PostgreSQL cloud vault
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO health_logs (user_id, data_type, raw_payload) VALUES (%s, %s, %s);",
            (user_id, data_type, json.dumps(payload))
        )
        conn.commit()
        cur.close()
        conn.close()
        
        # Send a success notification back to the device
        return {"status": "success", "message": "Saved to database!"}
        
    except Exception as e:
        print(f"Webhook Interception Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database Write Failure: {str(e)}")