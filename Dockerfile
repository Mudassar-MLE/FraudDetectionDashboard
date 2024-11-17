FROM python:3.12-slim

WORKDIR /app

COPY . /app
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*
    
RUN pip install --no-cache-dir psycopg2 faker pandas

EXPOSE 5432

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
