import os
import json
import asyncpg
from fastapi import FastAPI, Request, HTTPException
from contextlib import asynccontextmanager

# We read your existing Railway DATABASE_URL variable
DATABASE_URL = os.getenv("DATABASE_URL")

# 1. Automated Table Creator (Runs on Application Startup)
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Application booting up... Connecting to database via asyncpg.")
    try:
        # Open a quick connection pool
        conn = await asyncpg.connect(DATABASE_URL)
        # Create the table if it doesn't exist
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS health_logs (
                id SERIAL PRIMARY KEY,
                user_id TEXT,
                data_type TEXT,
                raw_payload JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await conn.close()
        print("Database table verification complete: 'health_logs' table is ready.")
    except Exception as e:
        print(f"CRITICAL ERROR during startup database configuration: {str(e)}")
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
        
        # Connect and insert the record asynchronously
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute(
            """
            INSERT INTO health_logs (user_id, data_type, raw_payload) 
            VALUES ($1, $2, $3);
            """,
            user_id, data_type, json.dumps(payload)
        )
        await conn.close()
        
        return {"status": "success", "message": "Saved to database via asyncpg!"}
        
    except Exception as e:
        print(f"Webhook Interception Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database Write Failure: {str(e)}")