# # Usa un'immagine base di Miniconda
# FROM continuumio/miniconda3

# # Imposta la working directory
# WORKDIR /app

# RUN apt-get update && apt-get install -y netcat-openbsd


# # Copia la cartella dei test e il file .env nella directory /app
# COPY tests /app/tests
# COPY build /app/build
# #COPY .env . 

# # Copia il file environment.yml per installare le dipendenze
# COPY ./environment.yml . 

# # Installa le dipendenze usando conda
# RUN conda env update -f ./environment.yml -n base

# # Imposta PYTHONPATH per includere la cartella src all'interno di build/src
# #ENV PYTHONPATH=build/src

# # Verifica la struttura delle cartelle e se i file sono stati copiati correttamente
# RUN ls -la /app
# RUN ls -la /app/build || echo "Directory /app/build does not exist"
# RUN ls -la /app/build/src || echo "Directory /app/build/src does not exist"
# RUN chmod -R 777 /app/build/src/log

# # Esegui i test con pytest, assicurati che PYTHONPATH includa la cartella src
# RUN PYTHONPATH=/app/build/src pytest /app/tests --maxfail=1 --disable-warnings -v -s


# # Espone una porta per il servizio se necessario (opzionale per i test)
# EXPOSE 5000

# # Imposta il punto di ingresso (solo se necessario avviare un'applicazione)
# ENTRYPOINT ["python3", "build/app.py", "&"]

FROM continuumio/miniconda3

WORKDIR /app

RUN apt-get update && apt-get install -y netcat-openbsd

# Export the proxy as env variable
ENV ENV=development

# Create the environment:
COPY ./environment.yml .
RUN conda env update -f ./environment.yml -n base

COPY . .

# Imposta PYTHONPATH per includere la cartella src
ENV PYTHONPATH=/app/build

# Esegui i test prima di eseguire il container
RUN pytest -v /app/tests --maxfail=1 --disable-warnings -s


# Expose port to be reached
EXPOSE 5000

# The code to run when container is started:
ENTRYPOINT ["python3", "build/app.py", "&"]
# ENTRYPOINT ["tail", "-f", "/dev/null"]
