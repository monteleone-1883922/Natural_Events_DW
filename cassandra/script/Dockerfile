FROM python:3.11-slim

# Installa le dipendenze necessarie
WORKDIR /app

# Copia i requirements e installa le dipendenze
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia lo script Python
COPY seed_data.py .

# Imposta il comando di default
ENTRYPOINT ["python3", "seed_data.py"]