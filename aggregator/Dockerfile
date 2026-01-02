FROM python:3.10-slim

WORKDIR /app
COPY app.py .

RUN pip install flask requests flask-cors

ENV PYTHONUNBUFFERED=1

CMD ["python", "app.py"]
