from flask import Flask, request, jsonify
from flask_cors import CORS
import requests

app = Flask(__name__)
CORS(app)

COMMONCRAWL_SERVICE = "http://127.0.0.1:5003/process"
COLCAP_SERVICE = "http://127.0.0.1:5001/colcap"

@app.route("/aggregate", methods=["GET"])
def aggregate():
    term = request.args.get("term")
    keyword = request.args.get("keyword")
    index = request.args.get("index")
    start = request.args.get("start")
    end = request.args.get("end")

    if not term:
        return jsonify({"error": "Missing term"}), 400

    response = {}

    # -------- CommonCrawl --------
    try:
        cc_params = {"term": term}
        if keyword:
            cc_params["keyword"] = keyword
        if index:
            cc_params["index"] = index
        if start:
            cc_params["start_date"] = start
        if end:
            cc_params["end_date"] = end

        print("[Aggregator] Enviando a CommonCrawl:", cc_params)
        cc_resp = requests.get(COMMONCRAWL_SERVICE, params=cc_params, timeout=180)
        print("[Aggregator] Respuesta CommonCrawl:", cc_resp.status_code)
        print("[Aggregator] JSON CommonCrawl:", cc_resp.json())
        response["commoncrawl"] = cc_resp.json()

    except Exception as e:
        print("CommonCrawl error:", e)
        response["commoncrawl"] = {"error": "CommonCrawl failed"}

    # -------- COLCAP --------
    try:
        if start and end:
            print("[Aggregator] Enviando a COLCAP:", {"start": start, "end": end})
            colcap_resp = requests.get(
                COLCAP_SERVICE,
                params={"start": start, "end": end},
                timeout=80
            )
            print("[Aggregator] Respuesta COLCAP:", colcap_resp.status_code)
            print("[Aggregator] JSON COLCAP:", colcap_resp.json())
            response["colcap"] = colcap_resp.json()
        else:
            response["colcap"] = []

    except Exception as e:
        print("COLCAP error:", e)
        response["colcap"] = []

    # -------- COMBINAR POR FECHA --------
    combined = []
    colcap_data = response.get("colcap", [])
    if isinstance(colcap_data, dict) and "data" in colcap_data:
        colcap_data = colcap_data["data"]
    cc = response.get("commoncrawl", {})
    news_by_date = {}
    if "date_ranges_counts" in cc:
        for date, count in cc["date_ranges_counts"]:
            news_by_date[date] = count
    else:
        total_news = cc.get("news_count") or cc.get("count") or 0
        for item in colcap_data:
            if isinstance(item, dict) and "date" in item:
                news_by_date[item["date"]] = total_news

    # --- AJUSTE PARA UN SOLO DATO ---
    if len(colcap_data) == 1 and "count" in cc:
        for item in colcap_data:
            if isinstance(item, dict) and "date" in item and "value" in item:
                date = item["date"]
                combined.append({
                    "date": date,
                    "news": cc["count"],  # usa el total de noticias encontradas
                    "colcap": item["value"]
                })
    else:
        for item in colcap_data:
            if isinstance(item, dict) and "date" in item and "value" in item:
                date = item["date"]
                combined.append({
                    "date": date,
                    "news": news_by_date.get(date, 0),
                    "colcap": item["value"]
                })

    print("[Aggregator] Datos combinados:", combined)
    response["combined"] = combined

    return jsonify(response)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002)