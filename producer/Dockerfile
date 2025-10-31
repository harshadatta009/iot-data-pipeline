FROM python:3.12-slim

WORKDIR /app
COPY producer.py .

# Install system and Python dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    liblz4-1 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir kafka-python loguru lz4

RUN mkdir -p /app/logs

ENV PYTHONUNBUFFERED=1 \
    LOGURU_LEVEL=INFO

CMD ["python", "producer.py"]
