import os
from flask import Flask

app = Flask(__name__)

@app.route("/")
def hello_world():
    # This print statement will show up in Cloud Logging
    print("LOG: Request received! Processing data...")
    
    # This string is returned to the caller (Browser or curl)
    return "VERSION 1: Hello from the Cloud Run Service!"

if __name__ == "__main__":
    # Cloud Run always sends the port via the PORT environment variable (default 8080)
    port = int(os.environ.get("PORT", 8080))
    app.run(debug=True, host="0.0.0.0", port=port)