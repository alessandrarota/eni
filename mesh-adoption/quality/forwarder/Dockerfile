FROM continuumio/miniconda3

WORKDIR /app

RUN apt-get update && apt-get install -y netcat-openbsd


# Create the environment:
COPY ./environment.yml .
RUN conda env update -f ./environment.yml -n base

COPY . .

# Imposta PYTHONPATH per includere la cartella src
ENV PYTHONPATH=/app/src

# Esegui i test prima di eseguire il container
RUN pytest -v /app/tests --maxfail=1 --disable-warnings -s

# Expose port to be reached
EXPOSE 5000

# The code to run when container is started:
ENTRYPOINT ["python3", "src/main.py", "&"]
