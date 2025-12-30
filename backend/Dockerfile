FROM python:3.11-slim

WORKDIR /app

# Installation des dépendances système
RUN apt-get update && apt-get install -y build-essential

# INSTALLATION FORCÉE DES WEBSOCKETS (On ne passe pas par le fichier ici pour être sûr)
RUN pip install --no-cache-dir uvicorn[standard] websockets fastapi confluent-kafka python-dotenv

# On copie le reste pour les autres dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

# Utilisation de uvicorn standard explicitement
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]