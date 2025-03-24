FROM jupyter/base-notebook

# Passa a root per installare pacchetti
USER root

# Installa Java (necessario per PySpark)
RUN apt-get update && \
    apt-get install -y default-jre default-jdk

# Imposta JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Ritorna all'utente jupyter per evitare problemi con i permessi
USER jovyan
