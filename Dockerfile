FROM python:3.11-slim

RUN pip install --no-cache-dir duckdb>=1.5.2

COPY server/ /app/server/
COPY scripts/ /app/scripts/

WORKDIR /app

CMD ["python", "-m", "server.main"]
