FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN apt-get update && \
    apt-get install -y default-jre default-jdk

ENV JAVA_HOME=/usr/lib/jvm/default-java

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

#CMD ["python", "gx_manage_result.py"]
CMD ["python", "custom_expectations.py"]
