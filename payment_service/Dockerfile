FROM python:3.9-slim

ENV PYTHONUNBUFFERED 1

WORKDIR /app

RUN apt-get update && apt-get install -y netcat-traditional

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Даем права на исполнение скрипту
RUN chmod +x /app/entrypoint.sh

# Запускаем скрипт
ENTRYPOINT ["/app/entrypoint.sh"]