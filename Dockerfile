FROM python:3.10-slim

WORKDIR /app

# Установка зависимостей
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt


COPY . .


ENV PORT=2700
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "2700"]
