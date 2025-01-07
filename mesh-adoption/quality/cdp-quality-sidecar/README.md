# Quality Sidecar

Questo sidecar integra **Great Expectations** con **OpenTelemetry** per eseguire validazioni su DataFrame e monitorare le metriche, utilizzando una configurazione personalizzata, e generare dei risultati in formato OTLP.

## Struttura del progetto

Il progetto è strutturato come segue:

```
├── Dockerfile
├── requirements.txt
├── builds/
│   └── app.py
└── gx_setup/
    └── gx_dataframe.py
└── resources/
    └── gx_v0.1.json
    └── ...
```

### Descrizione di file e cartelle

- **Dockerfile**: Definisce l'immagine del container e le fasi di installazione delle dipendenze, esecuzione dei test, e configurazione dell'ambiente per l'esecuzione dell'applicazione.
- **requirements.txt**: Contiene tutte le dipendenze Python necessarie per il progetto, inclusi `great_expectations`, `opentelemetry`, `pandas` e `pytest`.
- **/builds/app.py**: Contiene l'intera logica del sidecar: legge il file JSON di configurazione, imposta e valida le aspettative di Great Expectations, e utilizza OpenTelemetry per monitorare le metriche.
- **/gx_setup/gx_dataframe.py**: Contiene funzioni per configurare Great Expectations per il suo utilizzo sui DataFrame nello specifico, inclusa l'aggiunta di sorgenti dati, asset, suite di aspettative e definizioni di validazioni.
- **/resources**: La cartella contiene i file JSON di configurazione per definire i dati da validare e le aspettative di Great Expectations.

## Navigazione del codice e struttura

### Great Expectations

#### File di configurazione

I file all'interno della cartella **/resources** contengono le configurazioni necessarie per la corretta esecuzione di **Great Expectations** all'interno del sidecar, come ad esempio le aspettative di validazione sui dati. Ogni file di configurazione definisce i controlli sui dataset di interesse per un prodotto di dati specifico.

Per convenzione, i file di configurazione vengono nominati includendo la versione, ad esempio:
gx_v<i>MAJOR.MINOR</i>.json

**NB:** 
* È **obbligatorio** fornire almeno un file di configurazione e specificarne il percorso come variabile di ambiente del container (***vedi la sezione di configurazione del container***).
* I file di configurazione possono essere versionati per mantenere lo storico delle configurazioni o sovrascritti di volta in volta, a seconda delle esigenze del progetto (l'applicazione punterà al file indicato nella variabile di ambiente)

##### Struttura del file JSON
Il file deve essere strutturato come segue:

- **data_product_name**: Il nome del data_product per cui vengono definite le aspettative
    - il data_product_name definito è uno solo perchè da specifica il sidecar è unico per data_product.
  
- **data_product_suites**: Una lista di ***ExpectationSuite***, in cui ogni suite rappresenta un insieme di aspettative da applicare a un determinato set di dati. Ogni suite deve presentare:
  - **physical_informations**: Contiene le informazioni fisiche del set di dati, che includono:
    - **data_source_name**: Il nome della sorgente dati.
    - **data_asset_name**: Il nome dell'asset dati.
    - **dataframe**: Un URL che punta a un file CSV che contiene i dati da validare.
        - NB: un'estensione del progetto protrebbe prevedere la creazione di estrattori standard di DataFrame; in seguito questo campo potrà contenere il riferimento diretto al DataFrame interessato.
  - **expectations**: Una lista di aspettative da applicare sui dati. Ogni aspettativa contiene:
    - **expectation_name**: Il nome custom dell'aspettativa.
    - **expectation_type**: Il tipo di aspettativa da applicare (ad esempio, verifica che un valore non sia nullo, che un valore sia compreso tra due estremi, o che un valore corrisponda a una regex).
        - Consultare la gallery di Expectations ufficiale: https://greatexpectations.io/expectations/.
    - **kwargs**: I parametri necessari per eseguire l'aspettativa (ad esempio, il nome della colonna da verificare o i valori di limite per un intervallo).

<br><br>

Di seguito un esempio di file di configurazione:
        
```json
{
    "data_product_name": "consuntiviDiProduzione",
    "data_product_suites": [
        {
            "physical_informations": {
                "data_source_name": "cdpDataSourceSample",
                "data_asset_name": "cdpDataAssetSample",
                "dataframe": "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
            },
            "expectations": [
                {
                    "expectation_name": "expectVendorIdValuesToNotBeNull",
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {
                        "column": "vendor_id"
                    }
                },
                {
                    "expectation_name": "expectPassengerCountValuesToBeBetween",
                    "expectation_type": "expect_column_values_to_be_between",
                    "kwargs": {
                        "column": "passenger_count",
                        "min_value": 0,
                        "max_value": 4
                    }
                },
                {
                    "expectation_name": "expectPickupDatetimeValuesToMatchRegex",
                    "expectation_type": "expect_column_values_to_match_regex",
                    "kwargs": {
                        "column": "pickup_datetime",
                        "regex": "^\\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])\\s([01]\\d|2[0-3]):([0-5]\\d):([0-5]\\d)$"
                    }
                }
            ]
        }
    ]
}
```

#### Validazione dei dati
Il risultato finale della validazione tramite Great Expectations conterrà i risultati di tutte le suite definite nel file. Per ogni suite definita nel file di configurazione, il sistema eseguirà un controllo sui dati e fornirà un esito che indica se le aspettative sono state rispettate o meno. Se una suite contiene più di una aspettativa, ognuna di esse verrà validata individualmente, e il risultato finale conterrà un riepilogo di tutte le aspettative eseguite.


### OpenTelemetry

## Configurazione

### Variabili d'Ambiente

L'applicazione utilizza la variabile d'ambiente `EXPECTATIONS_JSON_FILE_PATH` per definire il percorso del file JSON delle aspettative. Assicurati che questa variabile punti al file di configurazione corretto (`resources/gx_v0.1.json`).

Esempio:

```bash
export EXPECTATIONS_JSON_FILE_PATH='build/resources/gx_v0.1.json'
```

### OpenTelemetry

L'applicazione è configurata per inviare metriche e log tramite OpenTelemetry. Le seguenti variabili d'ambiente sono utilizzate per configurare il livello di log e le opzioni di esportazione:

- **OTEL_LOG_LEVEL**: Imposta il livello di log (default: DEBUG).
- **OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED**: Abilita la strumentazione automatica del logging Python.
- **OTEL_PYTHON_DISABLED_INSTRUMENTATIONS**: Disabilita specifici strumenti di strumentazione (ad esempio, HTTP, requests).

## Esecuzione con Docker

1. **Costruisci l'immagine Docker**:
    ```bash
    docker build -t gx-opentelemetry-app .
    ```

2. **Esegui il container**:
    ```bash
    docker run -e EXPECTATIONS_JSON_FILE_PATH='/path/to/gx_v0.1.json' gx-opentelemetry-app
    ```

    Assicurati di sostituire il percorso con quello corretto del file JSON delle aspettative.

## Test

L'immagine Docker esegue automaticamente i test utilizzando `pytest` prima di avviare l'applicazione. Puoi eseguire i test manualmente anche senza Docker, se preferisci.

```bash
pip install -r requirements.txt
pytest -v /app/tests --maxfail=1 --disable-warnings -s
```

## Funzionalità

- **Lettura del file di configurazione JSON**: Il file JSON definisce le aspettative di validazione dei dati, che vengono eseguite sui dati caricati da un file CSV remoto.
- **Esecuzione delle validazioni**: Le validazioni vengono eseguite utilizzando Great Expectations, e i risultati vengono monitorati con OpenTelemetry.
- **Monitoraggio con OpenTelemetry**: Le metriche delle validazioni vengono raccolte e inviate tramite OpenTelemetry (esportazione a OTLP e console).