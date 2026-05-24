import sqlite3
import json
from fastapi import FastAPI, Request, HTTPException, Form
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager

DB_FILE = "health_data.db"

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
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
    except Exception as e:
        print(f"Startup Error: {str(e)}")
    yield

app = FastAPI(lifespan=lifespan)

# 1. BEAUTIFUL FRONTEND FORM (The Home Page)
@app.get("/", response_class=HTMLResponse)
async def home_page():
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Wearable Data Pipeline Portal</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f4f6f9; margin: 0; padding: 20px; display: flex; flex-direction: column; align-items: center; }
            .card { background: white; padding: 30px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.05); width: 100%; max-width: 450px; box-sizing: border-box; }
            h2 { margin-top: 0; color: #2c3e50; text-align: center; font-weight: 600; }
            p { text-align: center; color: #7f8c8d; font-size: 14px; margin-bottom: 25px; }
            label { font-weight: bold; display: block; margin: 15px 0 5px; color: #34495e; font-size: 14px; }
            input, select { width: 100%; padding: 12px; border: 1px solid #ccd1d9; border-radius: 6px; box-sizing: border-box; font-size: 15px; transition: all 0.3s; }
            input:focus, select:focus { border-color: #3498db; outline: none; box-shadow: 0 0 5px rgba(52,152,219,0.3); }
            button { width: 100%; background-color: #2ecc71; color: white; border: none; padding: 14px; border-radius: 6px; font-size: 16px; font-weight: bold; cursor: pointer; margin-top: 25px; transition: background 0.2s; }
            button:hover { background-color: #27ae60; }
            .nav-links { margin-top: 20px; display: flex; gap: 15px; }
            .nav-links a { color: #3498db; text-decoration: none; font-weight: bold; font-size: 14px; }
            .nav-links a:hover { text-decoration: underline; }
        </style>
    </head>
    <body>
        <div class="card">
            <h2>Stream Health Packet</h2>
            <p>Enter telemetry vitals to transmit into the cloud storage vault.</p>
            <form action="/submit-web" method="post">
                <label>User Identifier</label>
                <input type="text" name="user_id" placeholder="e.g., patient_beta_789" required>
                
                <label>Vitals Metric Type</label>
                <select name="data_type">
                    <option value="heart_rate">❤️ Heart Rate (BPM)</option>
                    <option value="step_count">🚶 Step Count</option>
                    <option value="oxygen_saturation">🫁 SpO2 Level</option>
                </select>
                
                <label>Vitals Value</label>
                <input type="number" name="metric_value" placeholder="e.g., 78" required>
                
                <button type="submit">🚀 Transmit to Pipeline</button>
            </form>
        </div>
        <div class="nav-links">
            <a href="/history" target="_blank">📊 View Live Database History</a>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

# 2. WEB FORM SUBMISSION PROCESSING HANDLER
@app.post("/submit-web")
async def handle_web_submission(user_id: str = Form(...), data_type: str = Form(...), metric_value: int = Form(...)):
    try:
        # Reconstruct structured JSON standard identical to wearable tracking telemetry packets
        payload = {
            "user": {"user_id": user_id},
            "type": data_type,
            "data": {"value": metric_value, "source": "web_portal_entry"}
        }
        
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO health_logs (user_id, data_type, raw_payload) VALUES (?, ?, ?);",
            (user_id, data_type, json.dumps(payload))
        )
        conn.commit()
        conn.close()
        
        # Simple, elegant successful transmission return page
        return HTMLResponse(content="""
            <body style="font-family:sans-serif; text-align:center; padding:50px; background:#f4f6f9;">
                <div style="background:white; padding:30px; border-radius:12px; display:inline-block; box-shadow:0 4px 10px rgba(0,0,0,0.05);">
                    <h2 style="color:#2ecc71;">✓ Transmission Successful!</h2>
                    <p style="color:#7f8c8d;">Data block committed seamlessly to database file.</p>
                    <br>
                    <a href="/" style="background:#3498db; color:white; padding:10px 20px; text-decoration:none; border-radius:5px; font-weight:bold;">← Send Another Packet</a>
                </div>
            </body>
        """)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# 3. HIGHLY STYLED LIVE DATA VIEWER DASHBOARD
@app.get("/history", response_class=HTMLResponse)
async def get_history_dashboard():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT id, user_id, data_type, raw_payload, created_at FROM health_logs ORDER BY id DESC;")
        rows = cursor.fetchall()
        conn.close()
        
        # Build table rows dynamically
        table_rows = ""
        for row in rows:
            payload_data = json.loads(row[3])
            inner_metrics = json.dumps(payload_data.get("data", {}))
            
            table_rows += f"""
            <tr>
                <td><strong>{row[0]}</strong></td>
                <td><span class="user-badge">{row[1]}</span></td>
                <td>{row[2]}</td>
                <td style="font-family: monospace; font-size: 13px; color: #555;">{inner_metrics}</td>
                <td style="color: #95a5a6; font-size: 13px;">{row[4]}</td>
            </tr>
            """
            
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Pipeline Database Dashboard</title>
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                body {{ font-family: 'Segoe UI', sans-serif; background-color: #f4f6f9; margin: 0; padding: 30px; }}
                .container {{ max-width: 1000px; margin: 0 auto; }}
                .header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 25px; }}
                h2 {{ color: #2c3e50; margin: 0; }}
                .refresh-btn {{ background: #3498db; color: white; text-decoration: none; padding: 10px 18px; border-radius: 5px; font-weight: bold; font-size: 14px; }}
                table {{ width: 100%; background: white; border-collapse: collapse; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 12px rgba(0,0,0,0.04); }}
                th, td {{ padding: 15px 20px; text-align: left; border-bottom: 1px solid #eef2f5; }}
                th {{ background-color: #34495e; color: white; font-weight: 600; text-transform: uppercase; font-size: 12px; letter-spacing: 0.5px; }}
                tr:hover {{ background-color: #f8fafc; }}
                .user-badge {{ background: #e8f4fd; color: #2980b9; padding: 4px 8px; border-radius: 4px; font-weight: bold; font-size: 13px; }}
                .empty {{ text-align: center; color: #95a5a6; padding: 40px; background: white; border-radius: 8px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h2>📊 Cloud Database Records Vault</h2>
                    <a href="/history" class="refresh-btn">🔄 Refresh Dashboard Data</a>
                </div>
                
                {"<table><tr><th width='8%'>ID</th><th width='25%'>User ID</th><th width='18%'>Metric Type</th><th>Payload Value</th><th width='22%'>Timestamp</th></tr>" + table_rows + "</table>" if rows else "<div class='empty'><h3>No Data Records Found</h3><p>Use the home input panel to push data packets into cyberspace.</p></div>"}
            </div>
        </body>
        </html>
        """
        return HTMLResponse(content=html_content)
    except Exception as e:
        return HTMLResponse(content=f"Error: {str(e)}", status_code=500)

# 4. Standard Backend JSON API (Keeps old terminal curl commands working seamlessly!)
@app.post("/webhook")
async def receive_data(request: Request):
    try:
        payload = await request.json()
        user_id = payload.get("user", {}).get("user_id", "unknown_user")
        data_type = payload.get("type", "unknown_type")
        
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
        raise HTTPException(status_code=500, detail=str(e))