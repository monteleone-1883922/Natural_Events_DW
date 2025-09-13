FROM python:3.11-slim

# Crea directory di lavoro
WORKDIR /app

# Copia requirements e installa dipendenze Python
COPY . .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /neo4j/data

ENTRYPOINT ["python3", "download_data.py"]
