# Usa una base di immagine (Python o Ubuntu, a seconda di cosa usi)
FROM python:3.11-slim

WORKDIR /app

# Installa le dipendenze Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN apt-get update && \
    apt-get install -y default-jre default-jdk

ENV JAVA_HOME=/usr/lib/jvm/default-java

#ENV PYTHONPATH=/app/quality_sidecar/gx:/app/quality_sidecar/otlp
ENV PYTHONPATH=/app/qualitysidecar/qualitysidecar

ENV EXPECTATIONS_JSON_FILE_PATH=resources/sapNac/csv_v0.1.json
ENV OTEL_METRIC_EXPORT_INTERVAL=10000
ENV DATA_PRODUCT_NAME=dataProductNameSample
ENV BUSINESS_DOMAIN_NAME=
ENV OTEL_SERVICE_NAME=${DATA_PRODUCT_NAME}-quality_sidecar

# Esegui i test prima di eseguire il container
RUN pytest -v /app/tests --maxfail=1 --disable-warnings -s

ENV OTEL_LOG_LEVEL=DEBUG \
    OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true

ENV OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=http,requests
CMD ["python", "quality.py"]