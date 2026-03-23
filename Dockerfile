FROM python:3.11

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
WORKDIR /app/backend

CMD gunicorn app:app --bind 0.0.0.0:${PORT:-5002} --workers 2 --timeout 120
