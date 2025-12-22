from flask import Flask, request, jsonify
from flask_cors import CORS
import requests

app = Flask(__name__)
CORS(app)

COMMONCRAWL_SERVICE = "http://commoncrawl:5000/process"
COLCAP_SERVICE = "http://colcap:5000/colcap"

@app.route("/aggregate", methods=["GET"])
def aggregate():
    term = request.args.get("term")
    index = request.args.get("index")  # 👈 NUEVO (opcional)

    if not term:
        return jsonify({"error": "Missing term"}), 400

    # 🔹 CommonCrawl
    try:
        cc_params = {"term": term}
        if index:
            cc_params["index"] = index

        news_resp = requests.get(
            COMMONCRAWL_SERVICE,
            params=cc_params,
            timeout=30
        )

        news_data = news_resp.json()
        news_count = news_data.get("news_count", 0)

    except Exception as e:
        print("Error CommonCrawl:", e)
        news_count = 0

    # 🔹 COLCAP
    try:
        colcap_data = requests.get(COLCAP_SERVICE, timeout=10).json()
    except Exception as e:
        print("Error COLCAP:", e)
        colcap_data = []

    merged = []
    for c in colcap_data:
        merged.append({
            "date": c.get("date", "N/A"),
            "news": news_count,
            "colcap": c.get("colcap", 0)
        })

    return jsonify(merged)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
