FROM python:3.12-slim

WORKDIR /app

# Обновить pip
RUN pip install --upgrade pip

# Установить зависимости заранее
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "seed.py"]
