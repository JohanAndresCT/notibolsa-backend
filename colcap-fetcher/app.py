import pandas as pd
from flask import Flask, jsonify

app = Flask(__name__)

DATA = {
    "date": ["2023-03", "2023-04", "2023-05"],
    "colcap": [1350, 1380, 1420]
}

df = pd.DataFrame(DATA)

@app.route("/colcap", methods=["GET"])
def get_colcap():
    return jsonify(df.to_dict(orient="records"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
