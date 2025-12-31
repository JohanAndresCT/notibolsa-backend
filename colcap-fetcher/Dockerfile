FROM python:3.10-slim

WORKDIR /app
COPY app.py .

RUN pip install flask cloudscraper yfinance pandas

CMD ["python", "app.py"]
