from flask import Flask, request
import json
import os
 
app = Flask(__name__)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>File Upload Trigger</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 20px;
        }
        .container {
            background: white;
            border-radius: 12px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            max-width: 500px;
            width: 100%;
            padding: 40px;
        }
        h1 {
            color: #333;
            margin-bottom: 10px;
            font-size: 28px;
        }
        .status {
            display: inline-block;
            background: #4CAF50;
            color: white;
            padding: 8px 16px;
            border-radius: 20px;
            font-size: 14px;
            margin-bottom: 30px;
        }
        .info-section {
            background: #f5f5f5;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
        }
        .info-section h2 {
            color: #667eea;
            font-size: 16px;
            margin-bottom: 12px;
        }
        .info-item {
            margin-bottom: 10px;
            font-size: 14px;
            color: #555;
        }
        .info-item strong {
            color: #333;
        }
        .endpoint {
            background: #f9f9f9;
            border-left: 4px solid #667eea;
            padding: 12px;
            margin: 10px 0;
            font-family: 'Courier New', monospace;
            font-size: 13px;
            color: #333;
            overflow-x: auto;
        }
        .badge {
            display: inline-block;
            background: #667eea;
            color: white;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 12px;
            margin-right: 5px;
        }
        footer {
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #eee;
            text-align: center;
            color: #999;
            font-size: 12px;
        }
        .tech-stack {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin-top: 12px;
        }
        .tech {
            background: white;
            border: 1px solid #ddd;
            padding: 6px 12px;
            border-radius: 4px;
            font-size: 12px;
            color: #555;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 File Upload Trigger</h1>
        <div class="status">✅ Service is Running</div>
        
        <div class="info-section">
            <h2>📊 Service Status</h2>
            <div class="info-item"><strong>Status:</strong> Healthy</div>
            <div class="info-item"><strong>Endpoint:</strong> /</div>
            <div class="info-item"><strong>Methods:</strong> GET, POST</div>
        </div>

        <div class="info-section">
            <h2>🔗 API Endpoints</h2>
            <div class="info-item">
                <span class="badge">GET</span>
                <strong>GET /</strong> - Returns status
            </div>
            <div class="endpoint">/ → OK</div>
            
            <div class="info-item" style="margin-top: 15px;">
                <span class="badge" style="background: #ff9800;">POST</span>
                <strong>POST /</strong> - Receives file upload triggers
            </div>
            <div class="endpoint">Logs JSON payload to console</div>
        </div>

        <div class="info-section">
            <h2>☁️ Deployment</h2>
            <div class="info-item"><strong>Platform:</strong> Google Cloud Run</div>
            <div class="info-item"><strong>Region:</strong> us-central1</div>
            <div class="info-item"><strong>CI/CD:</strong> Cloud Build</div>
        </div>

        <div class="info-section">
            <h2>🛠️ Tech Stack</h2>
            <div class="tech-stack">
                <div class="tech">Python 3.10</div>
                <div class="tech">Flask 3.0.0</div>
                <div class="tech">Docker</div>
                <div class="tech">Cloud Run</div>
                <div class="tech">Cloud Build</div>
                <div class="tech">GitHub</div>
            </div>
        </div>

        <footer>
            <p>Deployed with Cloud Build • Auto-scaling on Cloud Run</p>
            <p style="margin-top: 8px; font-size: 11px;">Push to GitHub → Cloud Build triggers → Auto-deploy to Cloud Run</p>
        </footer>
    </div>
</body>
</html>
"""

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "GET":
        return HTML_TEMPLATE
    elif request.method == "POST":
        print("✅ FILE UPLOAD TRIGGER RECEIVED")
        print(json.dumps(request.get_json(), indent=2))
        return "OK"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
 