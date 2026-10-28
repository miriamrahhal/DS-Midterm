
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY metadata_service.py .
COPY metadata_store.py .
COPY placement_api.py .
COPY chunking.py .
COPY hash_ring.py .
COPY storage_node.py .

EXPOSE 8000 8001
