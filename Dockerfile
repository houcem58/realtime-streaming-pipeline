FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

LABEL org.opencontainers.image.source="https://github.com/houcem58/realtime-streaming-pipeline"
LABEL org.opencontainers.image.description="Realtime Streaming Pipeline — Kafka drift detection"
LABEL org.opencontainers.image.licenses="Apache-2.0"

CMD ["python", "scripts/run_demo.py"]
