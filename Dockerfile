# Multi-stage Dockerfile extending shared Python template

# Build stage - extends shared Python template
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    SERVICE_NAME=data \
    SERVICE_TYPE=data \
    DATA_SERVICE_PORT=9001 \
    SERVICE_PORT=9001

WORKDIR /app

COPY requirements.txt requirements.txt
RUN python -m pip install --upgrade pip && \
    python -m pip install -r requirements.txt && \
    python -m pip install .

COPY . .

ENV PYTHONPATH=/app/src

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD python -c "import urllib.request,os;u=f'http://localhost:{os.getenv('SERVICE_PORT','9001')}/health';urllib.request.urlopen(u).read()" || exit 1

EXPOSE 9001

RUN useradd -u 1000 -m appuser && chown -R appuser /app
USER appuser

CMD ["python", "src/main.py"]
