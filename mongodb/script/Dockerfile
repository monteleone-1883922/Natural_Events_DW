FROM python:3.11-slim

# Installa le dipendenze necessarie
WORKDIR /app

# Copia i requirements e installa le dipendenze
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia lo script Python
COPY seed_data.py .

# Copia i dati CSV nella directory /app/data
COPY data/ ./data/

# Imposta il comando di default
CMD ["python", "seed_data.py"]