# Jupyter Notebook Sample

Questo progetto contiene un setup Docker per eseguire un ambiente Jupyter Notebook.

## Struttura del Progetto

- **`Dockerfile`**: Definisce l'immagine Docker per l'ambiente Jupyter Notebook.
- **`notebooks/`**: Cartella in cui vengono salvati i notebook creati. Questa directory viene montata come volume nel container, consentendo la persistenza dei dati tra i riavvii. Contiene anche eventuali file di cache generati durante l'esecuzione.

## Requisiti

- [Docker](https://www.docker.com/) installato e configurato.

## Configurazione

Il file `Dockerfile` utilizza l'immagine base `jupyter/base-notebook` e:
- Imposta la directory di lavoro su `/home/jovyan`.
- Copia la cartella locale `notebooks` nella directory di lavoro del container.
- Esporta la porta `8888` per accedere a Jupyter.
- Utilizza una variabile d'ambiente `JUPYTER_TOKEN` per autenticarsi (di default impostata su `password`).

## Esecuzione

### 1. Costruire l'immagine Docker

Per creare l'immagine Docker, esegui il seguente comando dalla directory contenente il `Dockerfile`:

```bash
docker build -t jupyter-notebook .
```

### 2. Avviare il container

Per avviare il container, esegui il comando seguente:

```bash
docker run -d --name jupyter-notebook -p 8888:8888 -v $(PWD)/notebooks:/home/jovyan/notebooks jupyter-notebook
```

### 3. Accedere all'interfaccia Jupyter Notebook

Andare all'indirizzo: [http://localhost:8888](http://localhost:8888).

**NB:** al  **primo avvio**, Jupyter ti chiederà di inserire una password: di default è `password` (si può modificare nel file Dockerfile). Non sarà richiesto di inserire nuovamente la password nei riavvi successivi, poiché Jupyter genera automaticamente un file di configurazione nella cartella `notebooks` che contiene le credenziali salvate.