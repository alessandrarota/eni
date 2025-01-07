# Usa una base di immagine (Python o Ubuntu, a seconda di cosa usi)
FROM python:3.9-slim

WORKDIR /app

# Installa le dipendenze Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV EXPECTATIONS_JSON_FILE_PATH='build/resources/gx_v0.1.json'

COPY . .

ENV PYTHONPATH=/app/build

# Esegui i test prima di eseguire il container
RUN pytest -v /app/tests --maxfail=1 --disable-warnings -s

ENV OTEL_LOG_LEVEL=DEBUG \
    OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true

ENV OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=http,requests

CMD ["opentelemetry-instrument", "--metrics_exporter", "otlp,console", "--logs_exporter", "otlp,console", "python", "build/app.py"]
