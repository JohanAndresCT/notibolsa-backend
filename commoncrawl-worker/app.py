import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

def normalize(text):
    return (
        text.lower()
        .replace("á","a")
        .replace("é","e")
        .replace("í","i")
        .replace("ó","o")
        .replace("ú","u")
    )

@app.route("/process", methods=["GET"])
def process():
    term = request.args.get("term")
    year = request.args.get("year")
    month = request.args.get("month")

    if not term or not year or not month:
        return jsonify({"error": "Missing parameters"}), 400

    term_clean = normalize(term)
    index = f"https://index.commoncrawl.org/CC-NEWS-{year}-{month}"

    url = (
        f"{index}"
        f"?url=*{term_clean}*"
        f"&output=json"
    )

    response = requests.get(url, timeout=30)

    count = 0
    if response.status_code == 200:
        count = len(response.text.splitlines())

    return jsonify({
        "date": f"{year}-{month}",
        "term": term,
        "news_count": count
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
