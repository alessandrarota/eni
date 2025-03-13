FROM jupyter/base-notebook:latest

USER root

RUN apt-get update && \
    apt-get install -y default-jre default-jdk

ENV JAVA_HOME=/usr/lib/jvm/default-java

WORKDIR /home/jovyan/quality

# Copia il pacchetto qualitysidecar nel contenitore
#COPY ./quality-sidecar/qualitysidecar /home/jovyan/quality/qualitysidecar
COPY ./quality-sidecar /home/jovyan/quality

# Installa il pacchetto qualitysidecar
#RUN pip install /home/jovyan/quality/qualitysidecar
RUN pip install /home/jovyan/quality

EXPOSE 8888
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--no-browser", "--allow-root"]
