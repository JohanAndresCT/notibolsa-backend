FROM python:3.10-slim

WORKDIR /app
COPY app.py .

RUN pip install flask yfinance pandas

ENV PYTHONUNBUFFERED=1

CMD ["python", "app.py"]
