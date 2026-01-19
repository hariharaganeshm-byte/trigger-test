from flask import Flask, request
import json
import os
 
app = Flask(__name__)

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "GET":
        return "OK", 200
    elif request.method == "POST":
        print("✅ FILE UPLOAD TRIGGER RECEIVED")
        print(json.dumps(request.get_json(), indent=2))
        return "OK"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
 