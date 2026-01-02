from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf
from flask import Flask, jsonify, request

app = Flask(__name__)


@app.route("/colcap", methods=["GET"])
def get_colcap():
    start_date = request.args.get("start")
    end_date = request.args.get("end")

    if not start_date or not end_date:
        return jsonify({"error": "Missing date parameters"}), 400

    # Validar formato YYYY-MM-DD
    try:
        print(f"start_date recibido: '{start_date}'")
        print(f"end_date recibido: '{end_date}'")
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

    TICKER = "^737809-COP-STRD"

    try:
        df = yf.download(
            TICKER,
            start=start_dt.strftime("%Y-%m-%d"),
            end=(end_dt + timedelta(days=1)).strftime("%Y-%m-%d"),
            interval="1d",
            progress=False,
        )
        print("DataFrame descargado:")
        print(df)
        print("Columnas del DataFrame:", df.columns)
        print("Índice del DataFrame:", df.index)
        # Filtrar explícitamente el rango solicitado
        if not df.empty:
            # Asegurar que el índice sea datetime y normalizar fechas para ignorar zona horaria
            if not pd.api.types.is_datetime64_any_dtype(df.index):
                df.index = pd.to_datetime(df.index)
            df.index = df.index.normalize()
            start_norm = pd.to_datetime(start_dt).normalize()
            end_norm = pd.to_datetime(end_dt).normalize()
            df = df[(df.index >= start_norm) & (df.index <= end_norm)]

        # Fallback: último valor disponible
        if df.empty:
            df = yf.Ticker(TICKER).history(period="1d")

        # Si sigue vacío → respuesta válida, no error
        if df.empty:
            return jsonify(
                {
                    "data": [],
                    "warning": "No historical data available, only last value exists",
                }
            ), 200

        # Aplanar columnas si vienen como MultiIndex
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        result = []
        for index, row in df.iterrows():
            if "Close" in row and pd.notna(row["Close"]):
                result.append(
                    {"date": index.strftime("%Y-%m-%d"), "value": float(row["Close"])}
                )

        return jsonify({"ticker": TICKER, "count": len(result), "data": result}), 200

    except Exception as e:
        return jsonify({"error": "Internal server error", "detail": str(e)}), 500


if __name__ == "__main__":
    import yfinance as yf

    print(f"Versión de yfinance: {yf.__version__}")
    app.run(host="0.0.0.0", port=5001)
