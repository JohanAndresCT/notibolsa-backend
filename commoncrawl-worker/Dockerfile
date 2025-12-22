FROM python:3.10-slim

WORKDIR /app
COPY app.py .


RUN pip install flask requests beautifulsoup4 lxml warcio \
	&& apt-get update && apt-get install -y curl iputils-ping \
	&& rm -rf /var/lib/apt/lists/*

CMD ["python", "app.py"]
