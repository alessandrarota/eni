# Jupyter Notebook Sample

Questo progetto contiene un setup Docker per eseguire un ambiente Jupyter Notebook.

## Struttura del Progetto

- **`docker-compose.yml`**: Definisce il compoe per l'ambiente Jupyter Notebook.
- **`notebooks/`**: Cartella in cui vengono salvati i notebook creati. Questa directory viene montata come volume nel container, consentendo la persistenza dei dati tra i riavvii. Contiene anche eventuali file di cache generati durante l'esecuzione.

## Requisiti

- [Docker](https://www.docker.com/) installato e configurato.

## Esecuzione

Eseguire il comando:

```bash
docker compose up --build
```

Andare all'indirizzo: [http://localhost:8888](http://localhost:8888).

**NB:** al  **primo avvio**, Jupyter ti chiederà di inserire una password: di default è `password`. Non sarà richiesto di inserire nuovamente la password nei riavvi successivi, poiché Jupyter genera automaticamente un file di configurazione nella cartella `notebook` che contiene le credenziali salvate.