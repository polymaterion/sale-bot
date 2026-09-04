FROM python:3.12-slim

# Системные зависимости для tzdata
RUN apt-get update && apt-get install -y --no-install-recommends \
        tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py texts.py main.py ./

# Папка для SQLite базы данных (монтируется как volume)
RUN mkdir -p /data

CMD ["python", "-u", "main.py"]
