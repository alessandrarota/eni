FROM jupyter/base-notebook:latest

USER root
WORKDIR /home/jovyan/quality

# Copia il pacchetto qualitysidecar nel contenitore
COPY ./quality-sidecar/qualitysidecar /home/jovyan/quality/qualitysidecar

# Installa il pacchetto qualitysidecar
RUN pip install /home/jovyan/quality/qualitysidecar

EXPOSE 8888
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--no-browser", "--allow-root"]
