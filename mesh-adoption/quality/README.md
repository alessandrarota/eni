# Data Quality

Questo repository contiene tutte le componenti necessarie per implementare il processo di data quality.

## Struttura del progetto
Il progetto è organizzato come segue:
- cartella `/quality-sidecar` 
- cartella `/platform-collector`  
- cartella `/platform-quality-receiver` 
- cartella `/platform-local-metastore` 
- cartella `/forwarder` 
- cartella `/notebook` 
- cartella `/bd-quality-suite-check-creation`  
- file `.env`  - File di configurazione delle variabili d'ambiente
- file `docker-compose.yml`  - File per l'orchestrazione dei servizi Docker


## Navigazione del codice e struttura
Di seguito è riportata una descrizione di ogni componente.

### Quality Sidecar
Il Quality Sidecar integra due moduli principali, **GX (Great Expectations)** e **OTLP (OpenTelemetry Protocol)**, per gestire e monitorare la qualità dei dati di un prodotto. L'obiettivo è applicare controlli di qualità sui dati tramite Great Expectations e inviare le metriche di qualità tramite il protocollo OpenTelemetry, utilizzando l'SDK Python per OTLP.

Nella cartella `quality-sidecar` è presente il `README.md` con i dettagli.

**Nota**: La cartella `notebook` contiene i file necessari per il deployment di un'istanza di Jupyter Notebook, utilizzata per simulare l'ambiente Databricks in Eni. La cartella `quality-sidecar` all'interno è identica a quella a livello di progetto.

### OTLP Collector
Il OpenTelemetry Collector raccoglie, elabora ed esporta le metriche ricevute dal quality sidecar. Tutto ciò avviene tramite il protocollo OTLP, sia in modalità gRPC sulla porta 4317 che in modalità HTTP sulla porta 4318.

### Quality Receiver
Il Quality Receiver è un'applicazione Spring Boot che utilizza un server gRPC per elaborare le metriche ricevute da OTLP Collector.
Quando vengono ricevute delle metriche, il servizio le esamina e, a seconda del tipo, estrae i dati associati. Se il tipo di segnale è valido (`"signal_type": "DATA_QUALITY"`), i dati vengono salvati sul metastore locale, piu precisamente nella tabella `metric_current`.

### Local Metastore
Il Local Metastore include un database denominato `quality`, con all'interno due tabelle, `metric_current` e `metric_history`, che memorizzano informazioni sulle metriche raccolte. Di seguito sono riportati i DDL delle tabelle:
```SQL
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'metric_current')
BEGIN
        CREATE TABLE metric_current (
            data_product_name VARCHAR(255) NULL,
            check_name VARCHAR(255) NOT NULL,
            metric_value FLOAT NULL,
            unit_of_measure VARCHAR(8) NULL,
            checked_elements_nbr INT NULL,
            errors_nbr INT NULL,
            metric_source_name VARCHAR(255) NULL,
            status_code VARCHAR(255) NOT NULL,
            locking_service_code VARCHAR(255) NULL,
            otlp_sending_datetime_code VARCHAR(14) NOT NULL,
            otlp_sending_datetime TIMESTAMP NOT NULL,
            insert_datetime TIMESTAMP NOT NULL,
            update_datetime TIMESTAMP NOT NULL,
            CONSTRAINT PK_metric_current PRIMARY KEY (check_name, otlp_sending_datetime_code)
        );
END
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'metric_history')
BEGIN
    CREATE TABLE metric_history (
            data_product_name VARCHAR(255) NULL,
            check_name VARCHAR(255) NOT NULL,
            metric_value FLOAT NULL,
            unit_of_measure VARCHAR(8) NULL,
            checked_elements_nbr INT NULL,
            errors_nbr INT NULL,
            metric_source_name VARCHAR(255) NULL,
            status_code VARCHAR(255) NOT NULL,
            locking_service_code VARCHAR(255) NULL,
            otlp_sending_datetime_code VARCHAR(14) NOT NULL,
            otlp_sending_datetime TIMESTAMP NOT NULL,
            insert_datetime TIMESTAMP NOT NULL,
            update_datetime TIMESTAMP NOT NULL,
            CONSTRAINT PK_metric_history PRIMARY KEY (check_name, otlp_sending_datetime_code)
        );
END
GO
```
**Nota:** in locale è stata utilizzata un'immagine SQL-Server (mcr.microsoft.com/mssql/server:2019-latest). In ambiente Eni il metastore locale risiede su un PostgreSQL.

### Blindata Forwarder
Il Blindata Forwarder e' uno scheduler che ciclicamente legge le metriche contenute nella tabella `metric_current`, invia ciascuna metrica verso blindata e storicizza il risultato nella tabella `metric_history`.

Vengono gestiti diversi tipi di `status_code`:
- **NEW**: nuova metrica inserita (tabella `metric_current`)
- **LOCKED**: un'istanza di Blindata Forwarder blocca le metriche non gia bloccate da altre istanze (tabella `metric_current`)
- **SUCCESS**: metriche correttamente inviate a Blindata (tabella `metric_history`)
- **ERR_CHECK_NOT_FOUND**: quality check non trovato su Blindata, quindi non è stato possibile inviare il risultato della quality (tabella `metric_history`)
- **ERR_BLINDATA_<response_code>**: errore generico che Blindata può restituire a seguito del caricamento dei risultati (tabella `metric_history`)

### Blindata Quality Suite/Check Creation
Questo modulo è escluso dal docker-compose.yml. Serve per creare le quality suite e i quality check in Blindata a partire da un file excel, nel quale devono essere inseriti i riferimenti per creare suite, check e associazioni fisiche sui check.
