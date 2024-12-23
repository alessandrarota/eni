# Usa una base di immagine (Python o Ubuntu, a seconda di cosa usi)
FROM python:3.9-slim

# Installa le dipendenze Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia il codice dell'app nel container
COPY . /app

# Imposta la directory di lavoro
WORKDIR /app

ENV OTEL_LOG_LEVEL=DEBUG \
    OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true

ENV OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=http,requests

CMD ["opentelemetry-instrument", "--metrics_exporter", "otlp,console", "--logs_exporter", "otlp,console", "python", "app.py"]
