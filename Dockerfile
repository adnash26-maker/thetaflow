FROM python:3.11-slim

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5002

CMD cd backend && gunicorn app:app --bind 0.0.0.0:${PORT:-5002} --workers 2 --timeout 120
