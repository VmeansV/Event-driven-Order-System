#!/bin/sh

# Ожидание доступности базы данных
echo "Waiting for postgres..."
while ! nc -z $DB_HOST $DB_PORT; do
  sleep 0.1
done
echo "PostgreSQL started"

# Применяем миграции
python manage.py migrate --noinput

# Запускаем Kafka-консьюмера в фоновом режиме
echo "Starting Kafka consumer..."
python manage.py run_kafka_consumer &

# Запускаем веб-сервер
echo "Starting Gunicorn server..."
gunicorn --bind 0.0.0.0:8000 --workers 3 payment_project.wsgi:application