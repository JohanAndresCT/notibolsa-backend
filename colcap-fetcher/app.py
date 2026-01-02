import json

import cloudscraper
from flask import Flask, jsonify, request

app = Flask(__name__)


@app.route("/colcap", methods=["GET"])
def get_colcap():
    start_date = request.args.get("start")
    end_date = request.args.get("end")
    if not start_date or not end_date:
        return jsonify({"error": "Missing date parameters"}), 400

    url = f"https://api.investing.com/api/financialdata/historical/49642?start-date={start_date}&end-date={end_date}&time-frame=Daily&add-missing-rows=false"
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "es-419,es;q=0.9",
        "Domain-id": "es",
        "Origin": "https://es.investing.com",
        "Referer": "https://es.investing.com/",
    }
    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "mobile": False}
    )
    try:
        response = scraper.get(url, headers=headers)
        if response.status_code != 200:
            return jsonify({"error": "Failed to fetch data"}), 500
        datos = json.loads(response.text)
        result = []
        for item in datos.get("data", []):
            valor = str(item["last_close"]).replace(".", "").replace(",", ".")
            try:
                valor = float(valor)
            except Exception:
                valor = None
            result.append({"date": item["rowDate"], "value": valor})
        return jsonify({"ticker": "COLCAP", "count": len(result), "data": result}), 200
    except Exception as e:
        return jsonify({"error": "Internal server error", "detail": str(e)}), 500


if __name__ == "__main__":
    import os

    import yfinance as yf

    print(f"Versión de yfinance: {yf.__version__}")
    port = int(os.getenv("PORT", 5001))
    app.run(host="0.0.0.0", port=port)
