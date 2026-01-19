from flask import Flask, request, render_template_string
import json
import os
import io
import base64
from datetime import datetime
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery

app = Flask(__name__)

# In-memory record of recent uploads (max 10)
recent_uploads = []
recent_ingests = []

PROJECT_ID = os.environ.get("PROJECT_ID")
BQ_DATASET = os.environ.get("BQ_DATASET")
BQ_TABLE = os.environ.get("BQ_TABLE")

storage_client = storage.Client() if PROJECT_ID else None
bq_client = bigquery.Client() if PROJECT_ID else None

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>File Upload Trigger</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 20px; }
        .container { background: white; border-radius: 12px; box-shadow: 0 20px 60px rgba(0,0,0,0.3); max-width: 960px; width: 100%; padding: 36px; }
        h1 { color: #333; margin-bottom: 10px; font-size: 28px; }
        .status { display: inline-block; background: #4CAF50; color: white; padding: 8px 16px; border-radius: 20px; font-size: 14px; margin-bottom: 24px; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 16px; }
        .panel { background: #f7f7fb; padding: 16px; border-radius: 10px; border: 1px solid #eee; }
        .panel h2 { color: #667eea; font-size: 16px; margin-bottom: 10px; }
        .info-item { margin-bottom: 8px; font-size: 14px; color: #444; }
        .badge { display: inline-block; background: #667eea; color: white; padding: 4px 8px; border-radius: 4px; font-size: 12px; margin-right: 6px; }
        form { margin-top: 8px; }
        .file-box { padding: 12px; border: 2px dashed #ccd4ff; border-radius: 10px; background: #f9f9ff; }
        .submit { margin-top: 12px; background: #667eea; color: white; padding: 10px 14px; border: none; border-radius: 6px; cursor: pointer; }
        table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 13px; }
        th, td { border: 1px solid #e0e0e0; padding: 6px 8px; text-align: left; }
        th { background: #eef0ff; }
        .uploads { margin-top: 16px; }
        footer { margin-top: 24px; padding-top: 16px; border-top: 1px solid #eee; text-align: center; color: #999; font-size: 12px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 File Upload Trigger</h1>
        <div class="status">✅ Service is Running</div>

        <div class="grid">
            <div class="panel">
                <h2>📊 Service Status</h2>
                <div class="info-item"><strong>Status:</strong> Healthy</div>
                <div class="info-item"><strong>Endpoint:</strong> /</div>
                <div class="info-item"><strong>Methods:</strong> GET, POST</div>
            </div>

            <div class="panel">
                <h2>🔗 API Endpoints</h2>
                <div class="info-item"><span class="badge">GET</span> <strong>/</strong> UI + status</div>
                <div class="info-item"><span class="badge" style="background:#ff9800;">POST</span> <strong>/</strong> Upload CSV / Excel</div>
                <div class="info-item"><span class="badge" style="background:#009688;">POST</span> <strong>/hook</strong> Pub/Sub JSON trigger (GCS)</div>
            </div>

            <div class="panel">
                <h2>☁️ Deployment</h2>
                <div class="info-item"><strong>Platform:</strong> Google Cloud Run</div>
                <div class="info-item"><strong>Region:</strong> us-central1</div>
                <div class="info-item"><strong>CI/CD:</strong> Cloud Build</div>
            </div>

            <div class="panel">
                <h2>🛠️ Tech Stack</h2>
                <div class="info-item">Python 3.10 • Flask 3.0.0 • Gunicorn</div>
                <div class="info-item">Docker • Cloud Run • Cloud Build</div>
                <div class="info-item">CSV/Excel parsing with pandas</div>
                <div class="info-item">GCS ingest → BigQuery</div>
            </div>
        </div>

        <div class="panel" style="margin-top:16px;">
            <h2>📂 Upload CSV / Excel</h2>
            <form method="POST" enctype="multipart/form-data">
                <div class="file-box">
                    <input type="file" name="file" accept=".csv,.xls,.xlsx" required />
                </div>
                <button class="submit" type="submit">Upload & Preview</button>
            </form>
            {% if message %}
            <div class="info-item" style="margin-top:10px;"><strong>{{ message }}</strong></div>
            {% endif %}
            {% if preview %}
            <div class="uploads">
                <div class="info-item"><strong>File:</strong> {{ preview.name }} ({{ preview.rows }} rows, {{ preview.cols }} cols)</div>
                <div class="info-item"><strong>Preview (first {{ preview.sample_rows|length }} rows):</strong></div>
                <table>
                    <thead>
                        <tr>
                        {% for col in preview.columns %}<th>{{ col }}</th>{% endfor %}
                        </tr>
                    </thead>
                    <tbody>
                        {% for row in preview.sample_rows %}
                        <tr>{% for cell in row %}<td>{{ cell }}</td>{% endfor %}</tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
            {% endif %}
        </div>

        <div class="panel" style="margin-top:16px;">
            <h2>🕑 Recent Uploads</h2>
            {% if uploads %}
            <table>
                <thead><tr><th>When</th><th>Name</th><th>Type</th><th>Rows</th><th>Cols</th></tr></thead>
                <tbody>
                    {% for u in uploads %}
                    <tr>
                        <td>{{ u.timestamp }}</td>
                        <td>{{ u.name }}</td>
                        <td>{{ u.kind }}</td>
                        <td>{{ u.rows }}</td>
                        <td>{{ u.cols }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% else %}
            <div class="info-item">No uploads yet. Upload a CSV or Excel file to see a preview.</div>
            {% endif %}
        </div>

        <div class="panel" style="margin-top:16px;">
            <h2>🗄️ Recent Ingestions (GCS → BQ)</h2>
            {% if ingests %}
            <table>
                <thead><tr><th>When</th><th>Bucket/Object</th><th>Rows</th><th>Status</th></tr></thead>
                <tbody>
                    {% for g in ingests %}
                    <tr>
                        <td>{{ g.timestamp }}</td>
                        <td>{{ g.bucket }}/{{ g.name }}</td>
                        <td>{{ g.rows }}</td>
                        <td>{{ g.status }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% else %}
            <div class="info-item">No ingestions yet. GCS upload should trigger /hook via Pub/Sub.</div>
            {% endif %}
        </div>

        <footer>
            <p>Deployed with Cloud Build • Auto-scaling on Cloud Run</p>
            <p style="margin-top: 8px; font-size: 11px;">Push to GitHub → Cloud Build triggers → Auto-deploy to Cloud Run</p>
        </footer>
    </div>
</body>
</html>
"""


def parse_upload(file_storage):
    """Parse CSV or Excel upload and return metadata plus sample rows."""
    filename = file_storage.filename or "upload"
    ext = filename.lower()
    data = file_storage.read()
    buf = io.BytesIO(data)

    if ext.endswith(".csv"):
        df = pd.read_csv(buf)
        kind = "csv"
    elif ext.endswith(".xls") or ext.endswith(".xlsx"):
        df = pd.read_excel(buf)
        kind = "excel"
    else:
        raise ValueError("Unsupported file type. Upload .csv or .xlsx")

    rows, cols = df.shape
    sample_rows = df.head(5).fillna("").astype(str).values.tolist()
    columns = list(df.columns.astype(str))
    return {
        "name": filename,
        "kind": kind,
        "rows": rows,
        "cols": cols,
        "columns": columns,
        "sample_rows": sample_rows,
    }


def load_to_bigquery(df, source_bucket, source_object):
    """Load CSV/Excel data to BigQuery and log metadata to ingestion_log table."""
    if not (PROJECT_ID and BQ_DATASET and bq_client):
        raise RuntimeError("BigQuery is not configured. Set PROJECT_ID, BQ_DATASET.")

    # Create table name from object name (sanitize: remove extension, replace invalid chars)
    import re
    table_name = re.sub(r'[^a-zA-Z0-9_]', '_', source_object.rsplit('.', 1)[0])
    data_table_id = f"{PROJECT_ID}.{BQ_DATASET}.{table_name}"
    
    # Load actual CSV/Excel data to dynamically named table
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite if exists
        autodetect=True,  # Auto-detect schema
    )
    job = bq_client.load_table_from_dataframe(df, data_table_id, job_config=job_config)
    job.result()  # wait for load
    rows_loaded = len(df)
    
    # Log metadata to ingestion_log table
    log_table_id = f"{PROJECT_ID}.{BQ_DATASET}.ingestion_log"
    log_df = pd.DataFrame([{
        "bucket": source_bucket,
        "object_name": source_object,
        "rows_loaded": rows_loaded,
        "status": "OK",
        "timestamp": pd.Timestamp.utcnow()
    }])
    log_job = bq_client.load_table_from_dataframe(log_df, log_table_id)
    log_job.result()
    
    return rows_loaded


def ingest_gcs_object(bucket_name, object_name):
    if not storage_client:
        raise RuntimeError("Storage client not configured. Set PROJECT_ID.")
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    data = blob.download_as_bytes()
    ext = object_name.lower()
    buf = io.BytesIO(data)

    if ext.endswith(".csv"):
        df = pd.read_csv(buf)
    elif ext.endswith(".xls") or ext.endswith(".xlsx"):
        df = pd.read_excel(buf)
    else:
        raise ValueError("Unsupported file type from GCS. Upload .csv or .xlsx")

    rows_loaded = load_to_bigquery(df, bucket_name, object_name)
    return rows_loaded, df.shape[1]


@app.route("/", methods=["GET", "POST"])
def index():
    preview = None
    message = None

    if request.method == "POST":
        uploaded = request.files.get("file")
        if not uploaded:
            message = "No file uploaded."
        else:
            try:
                preview = parse_upload(uploaded)
                recent_uploads.insert(0, {**preview, "timestamp": datetime.utcnow().isoformat() + "Z"})
                del recent_uploads[10:]
                message = "Upload parsed successfully."
            except Exception as exc:  # brief error message to UI
                message = f"Error: {exc}"

    return render_template_string(HTML_TEMPLATE, uploads=recent_uploads, preview=preview, message=message, ingests=recent_ingests)


@app.route("/hook", methods=["POST"])
def hook():
    payload = request.get_json(silent=True) or {}
    print("✅ JSON TRIGGER RECEIVED")
    print(json.dumps(payload, indent=2))

    # Expecting Pub/Sub push with message.data base64 containing GCS event
    bucket = ""
    name = ""
    try:
        msg = payload.get("message", {})
        data_b64 = msg.get("data")
        if not data_b64:
            raise ValueError("Missing Pub/Sub data")
        event_json = json.loads(base64.b64decode(data_b64).decode("utf-8"))
        bucket = event_json.get("bucket", "")
        name = event_json.get("name", "")
        if not bucket or not name:
            raise ValueError("Missing bucket/name in event")

        rows, cols = ingest_gcs_object(bucket, name)
        recent_ingests.insert(0, {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "bucket": bucket,
            "name": name,
            "rows": rows,
            "status": "OK"
        })
        del recent_ingests[10:]
        return "OK"
    except Exception as exc:
        recent_ingests.insert(0, {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "bucket": bucket,
            "name": name,
            "rows": 0,
            "status": f"ERROR: {exc}"
        })
        del recent_ingests[10:]
        print(f"ERROR: {exc}")
        return f"Error: {exc}", 400


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
 