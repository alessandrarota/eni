# Quality Sidecar


Questo sidecar integra **Great Expectations** con **OpenTelemetry** per eseguire validazioni su DataFrame e monitorare le metriche, utilizzando una configurazione personalizzata, e generare dei risultati in formato OTLP.

## Struttura del progetto

Il progetto è strutturato come segue:

```
├── builds/
│   └── app.py
└── gx_setup/
    └── gx_dataframe.py
└── resources/
    └── gx_v0.1.json
    └── ...
├── Dockerfile
├── requirements.txt
```

### Descrizione di file e cartelle

- **Dockerfile**: Definisce l'immagine del container e le fasi di installazione delle dipendenze, esecuzione dei test, e configurazione dell'ambiente per l'esecuzione dell'applicazione.
- **requirements.txt**: Contiene tutte le dipendenze Python necessarie per il progetto, inclusi `great_expectations`, `opentelemetry` e `pandas`.
- **/builds/app.py**: Contiene la logica principale del sidecar: legge il file JSON di configurazione, imposta e valida le aspettative di Great Expectations, e utilizza OpenTelemetry per monitorare le metriche.
- **/gx_setup/gx_dataframe.py**: Contiene funzioni per configurare Great Expectations per il suo utilizzo sui DataFrame nello specifico, inclusa l'aggiunta di sorgenti dati, asset, suite di aspettative e definizioni di validazioni.
- **/resources**: La cartella contiene i file JSON di configurazione per definire i dati da validare e le aspettative di Great Expectations.

## Navigazione del codice e struttura

### Great Expectations

#### File di configurazione

I file all'interno della cartella **/resources** contengono le configurazioni necessarie per la corretta esecuzione di **Great Expectations** all'interno del sidecar, come ad esempio le aspettative di validazione sui dati. Ogni file di configurazione definisce tutti i controlli sui dataset di interesse per l'intero data product.

Per convenzione, i file di configurazione vengono nominati includendo la versione, ad esempio:
gx_v<i>MAJOR.MINOR</i>.json

**NB:** 

- È **obbligatorio** fornire almeno un file di configurazione e specificarne il percorso come variabile di ambiente del container ([vai alla sezione specifica](#dettaglio-delle-variabili)).
- I file di configurazione possono essere versionati per mantenere lo storico delle configurazioni o sovrascritti di volta in volta, a seconda delle esigenze del progetto (l'applicazione punterà al file indicato nella variabile di ambiente).

##### Struttura del file JSON
Il file deve essere strutturato come segue:

- **data_product_name**: Il nome del data product per cui vengono definite le aspettative
    - il data_product_name definito è uno solo perchè da specifica il sidecar è unico per data product.
  
- **data_product_suites**: Una lista di **suite**, in cui ogni suite rappresenta un insieme di aspettative da applicare a un determinato set di dati. Ogni suite deve presentare:
  - **physical_informations**: Contiene le informazioni fisiche del set di dati, che includono:
    - **data_source_name**: Il nome della sorgente dati.
    - **data_asset_name**: Il nome dell'asset dati.
    - **dataframe**: Un URL che punta a un file CSV che contiene i dati da validare.
        - NB: un'estensione del progetto protrebbe prevedere la creazione di estrattori standard di DataFrame; in seguito questo campo potrà contenere il riferimento diretto al DataFrame interessato.
  - **expectations**: Una lista di aspettative da applicare sui dati. Ogni aspettativa contiene:
    - **expectation_name**: Il nome identificativo custom dell'aspettativa.
    - **expectation_type**: Il tipo di aspettativa da applicare (ad esempio, verifica che un valore non sia nullo, che un valore sia compreso tra due estremi, o che un valore corrisponda a una regex).
        - Consultare la gallery di Expectations ufficiale: https://greatexpectations.io/expectations/.
    - **kwargs**: I parametri necessari per eseguire l'aspettativa (ad esempio, il nome della colonna da verificare o i valori di limite per un intervallo).
      - Consultare la gallery di Expectations ufficiale: https://greatexpectations.io/expectations/.

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
Il risultato finale della validazione tramite Great Expectations conterrà i risultati di tutte le suite definite nel file. Per ogni suite definita nel file di configurazione, il sistema eseguirà un controllo sui dati e fornirà un esito che indica se le aspettative sono state rispettate o meno. Se una suite contiene più di una aspettativa, ognuna di esse verrà validata individualmente, e il risultato finale conterrà un riepilogo di tutte le aspettative eseguite sull'intero data product.


### OpenTelemetry

Opentelemetry viene utilizzato per raccogliere e inviare metriche, relative alla validazione dei dati con Great Expectations, tramite il protocollo OTLP a un sistema di monitoraggio esterno (es: Collector di piattaforma).

L'integrazione di OpenTelemetry avviene tramite i seguenti passaggi:

1. **Creazione di un ***Meter*****: Un oggetto ***Meter*** è creato, tramite la libreria opentelemetry.metrics, per raccogliere metriche.
2. **Definizione delle Metriche con ***ObservableGauge*****: Le metriche di validazione dei dati verranno raccolte tramite una metrica particolare chiamata ***ObservableGauge***. Questa metrica misurerà il valore percentuale di successo delle aspettative sui dati, e raccoglierà inoltre una serie di attributi custom (es: data_product_name, suite_name, ...).
3. **Raccolta delle Metriche**: Una funzione di callback che viene eseguita periodicamente (il tempo viene stabilito dal valore della variabile di ambiente specificata) per raccogliere le metriche di validazione. I risultati della validazione vengono mappati su valori percentuali che vengono esportati come metriche tramite OTLP.

#### Esempio di metrica in formato OTLP

Di seguito un esempio di risultato di validazione con Great Expectations esportato come metrica tramite OTLP:
``` json
{
  "resource_metrics":[
    {
      "resource":{
        "attributes":{
          "telemetry.sdk.language":"python",
          "telemetry.sdk.name":"opentelemetry",
          "telemetry.sdk.version":"1.28.2",
          "service.name":"consuntiviDiProduzione-quality_sidecar",
          "telemetry.auto.version":"0.49b2"
        },
        "schema_url":""
      },
      "scope_metrics":[
        {
          "scope":{
            "name":"__main__",
            "version":"",
            "schema_url":"",
            "attributes":null
          },
          "metrics":[
            {
              "name":"consuntividiproduzione-cdpdatasourcesample-cdpdataassetsample",
              "description":"Validation results for suite: consuntiviDiProduzione-cdpDataSourceSample-cdpDataAssetSample",
              "unit":"%",
              "data":{
                "data_points":[
                  {
                    "attributes":{
                      "element_count":10000,
                      "unexpected_count":667,
                      "expectation_name":"expectPassengerCountValuesToBeBetween",
                      "data_product_name":"consuntiviDiProduzione",
                      "suite_name":"consuntiviDiProduzione-cdpDataSourceSample-cdpDataAssetSample",
                      "data_source_name":"cdpDataSourceSample",
                      "data_asset_name":"cdpDataAssetSample"
                    },
                    "start_time_unix_nano":null,
                    "time_unix_nano":1736263323852875020,
                    "value":93.33,
                    "exemplars":[
                      
                    ]
                  },
                  {
                    "attributes":{
                      "element_count":10000,
                      "unexpected_count":0,
                      "expectation_name":"expectPickupDatetimeValuesToMatchRegex",
                      "data_product_name":"consuntiviDiProduzione",
                      "suite_name":"consuntiviDiProduzione-cdpDataSourceSample-cdpDataAssetSample",
                      "data_source_name":"cdpDataSourceSample",
                      "data_asset_name":"cdpDataAssetSample"
                    },
                    "start_time_unix_nano":null,
                    "time_unix_nano":1736263323852875020,
                    "value":100.0,
                    "exemplars":[
                      
                    ]
                  }
                ]
              }
            }
          ],
          "schema_url":""
        }
      ],
      "schema_url":""
    }
  ]
}
```

## Configurazione

Le variabili di ambiente per configurare il sidecar sono gestite tramite il file `docker-compose.yml`, mentre i valori effettivi vengono definiti in un file `.env` per semplificare la gestione e la modifica delle configurazioni.

### Dettaglio delle Variabili

1. **`OTEL_EXPORTER_OTLP_ENDPOINT`**: Specifica l'endpoint di destinazione per l'esportazione dei dati tramite OTLP (al collector di piattaforma).  
    ```env
    OTEL_EXPORTER_ENDPOINT=http://platform-collector:4317
    ```
    * `collector`: nome del container specificato per il collector di piattaforma nel `docker-compose.yml`.
    * `4317`: è la porta di defualt per il protocollo OTLP/gRPC.
    

2. **`OTEL_METRIC_EXPORT_INTERVAL`**: Intervallo in millisecondi per l'esportazione delle metriche OTLP (default: 60 secondi).
    ```env
    OTEL_METRIC_EXPORT_INTERVAL=60000
    ```

3. **`OTEL_SERVICE_NAME`**: Nome custom identificativo del servizio.
    ```env
    OTEL_SERVICE_NAME_CDP=consuntiviDiProduzione-quality_sidecar
    ```

4. **`EXPECTATIONS_JSON_FILE_PATH`**: Percorso del file JSON contenente le configurazioni per Great Expectations ([vai alla sezione specifica](#file-di-configurazione)).  
   ```env
    EXPECTATIONS_JSON_FILE_PATH=build/resources/gx_v0.1.json
    ```

## Esecuzione
Per eseguire l'applicazione è necessario avere installato [Docker Desktop](https://www.docker.com/).

Una volta clonato il progetto in locale e avviato Docker Desktop, posizionarsi all'interno della cartella sorgente del progetto ed eseguire il comando per creare l'immagine:
```
docker build -t sidecar .\
```
Successivamente, eseguire il comando per avviare il container sulla base dell'immagine appena creata:
```
docker run -d --name sidecar sidecar
```

**NB**: L'esecuzione stand-alone dell'applicazione è sconsigliata, poiché è progettata per funzionare insieme a un collector che riceva le metriche generate (nei log si potrà notare che l'applicazione tenterà sempre di connettersi al collector di default `localhost:4317`).  L'esecuzione dell'applicazione in modo indipendente può essere utile per testare i singoli test o per verificare il formato delle metriche generate, ma senza un collector attivo, la generazione delle metriche non ha alcun effetto pratico.

### Test
L'applicazione viene testata prima di ogni esecuzione grazie al comando all'interno del `Dockerfile`:
```
RUN pytest -v /app/tests --maxfail=1 --disable-warnings -s
```

I test sono stati creati mediante la libreria `pytest` al fine di testare le sole funzionalità di Great Expectations.