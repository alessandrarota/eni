# Quality Sidecar


Il quality sidecar integra due moduli principali, **GX (Great Expectations)** e **OTLP (OpenTelemetry Protocol)**, per gestire e monitorare la qualità dei dati di un prodotto. L'obiettivo è applicare controlli di qualità sui dati tramite Great Expectations, e inviare le metriche di qualità tramite protocollo e specifica OpenTelemetry, usando SDK Python per OTLP.

## Struttura del progetto

Il progetto è strutturato come segue:

```
├── quality.ipynb
├── /quality_sidecar
    └── /gx
        └── ...
    └── /otlp   
        └── ...
└── /resources  # Cartella per i file di configurazione e dati
    └── /<data_product>
        └── <expectations_file>.json  # File JSON con le aspettative di qualità per un prodotto specifico
        └── ... # Ulteriori file per il calcolo dei risultati (file csv)

```

La cartella `/quality_sidecar` contiene i moduli Python che gestiscono la logica di monitoraggio e validazione della qualità dei dati:
- `/gx`: gestisce la validazione dei dati usando Great Expectations
- `/otlp`: invia i risultati delle validazioni al sistema di monitoraggio tramite OpenTelemetry

Entrambe le cartelle contengono il file sorgente, a tendere potranno essere deployati come librerie indipendenti e importate direttamente nel notebook.

## Navigazione del codice e struttura

### Great Expectations

#### File di configurazione

I file all'interno della cartella **/resources** contengono le configurazioni necessarie per la corretta esecuzione di **Great Expectations** all'interno del sidecar, in particolare le aspettative di validazione sui dati. Ogni file di configurazione definisce tutti i controlli sui dataset di interesse per l'intero data product.

Per convenzione, i file di configurazione vengono nominati includendo la versione, ad esempio:
gx_v<i>MAJOR.MINOR</i>.json

**NB:** 

- È **obbligatorio** fornire almeno un file di configurazione e specificarne il percorso come variabile di ambiente del container ([vai alla sezione specifica](#dettaglio-delle-variabili)).
- I file di configurazione possono essere versionati per mantenere lo storico delle configurazioni o sovrascritti di volta in volta, a seconda delle esigenze del progetto (l'applicazione punterà al file indicato nella variabile di ambiente).

##### Struttura del file JSON
Il file di configurazione permette di definire tutti i controlli di data quality per l'intero data product. Contiene una lista di **suite**: ogni suite rappresenta un insieme di aspettative da applicare a un determinato asset di dati, all'interno di una sorgente. Ogni suite deve presentare el seguenti informazioni:

- **physical_informations**: Contiene le informazioni fisiche del set di dati, che includono:
    - **data_source_name**: Il nome della sorgente dati.
    - **data_asset_name**: Il nome dell'asset dati.
    - **dataframe**: Un URL che punta a un file CSV che contiene i dati da validare.
        - NB: Un'eventuale estensione del progetto potrebbe prevedere la creazione di estrattori standard per i DataFrame. In questo caso, il campo potrebbe non essere più necessario, poiché l'estrattore standard recupererebbe il DataFrame accedendo alla tabella tramite Unity Catalog, utilizzando i campi `data_source_name` e `data_asset_name`.
- **expectations**: Una lista di aspettative da applicare sui dati. Ogni aspettativa contiene:
    - **expectation_name**: Il nome del tipo di controllo che viene effettuato.
    - **expectation_type**: Il tipo di aspettativa da applicare (ad esempio, verifica che un valore non sia nullo, che un valore sia compreso tra due estremi, o che un valore corrisponda a una regex).
        - Consultare la gallery di Expectations ufficiale: https://greatexpectations.io/expectations/.
    - **kwargs**: I parametri necessari per eseguire l'aspettativa (ad esempio, il nome della colonna da verificare o i valori di limite per un intervallo).
      - Consultare la gallery di Expectations ufficiale: https://greatexpectations.io/expectations/.

Di seguito un esempio di file di configurazione:
        
```json
[
    {
        "physical_informations": {
            "data_source_name": "dataSourceSample",
            "data_asset_name": "dataAssetSample",
            "dataframe": "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
        },
        "expectations": [
            {
                "expectation_name": "checkNotNull",
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {
                    "column": "vendor_id"
                }
            },
            {
                "expectation_name": "checkAcceptedValues",
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "passenger_count",
                    "min_value": 0,
                    "max_value": 4
                }
            },
            {
                "expectation_name": "checkAcceptedValues",
                "expectation_type": "expect_column_values_to_match_regex",
                "kwargs": {
                    "column": "pickup_datetime",
                    "regex": "^\\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])\\s([01]\\d|2[0-3]):([0-5]\\d):([0-5]\\d)$"
                }
            }
        ]
    }
]
```

#### Validazione dei dati
Il risultato finale della validazione tramite Great Expectations conterrà i risultati di tutte le suite definite nel file. Per ogni suite definita nel file di configurazione, il sistema eseguirà un controllo sui dati e fornirà un esito che indica se le aspettative sono state rispettate o meno. Se una suite contiene più di una aspettativa, ognuna di esse verrà validata individualmente, e il risultato finale conterrà un riepilogo di tutte le aspettative eseguite sull'intero data product.

Di seguito un esempio di ```Validation Result```, con result_format="SUMMARY" (verbosità di default):
```json
{
  "success": false,
  "results": [
    {
      "success": true,
      "expectation_config": {
        "type": "expect_column_values_to_not_be_null",
        "kwargs": {
          "batch_id": "dataSourceSample-dataAssetSample",
          "column": "vendor_id"
        },
        "meta": {
          "expectation_name": "checkNotNull",
          "data_product_name": "consuntiviDiProduzione"
        },
        "id": "d3243727-a938-4b5a-b24a-ac8a6a993440"
      },
      "result": {
        "element_count": 10000,
        "unexpected_count": 0,
        "unexpected_percent": 0.0,
        "partial_unexpected_list": [],
        "partial_unexpected_counts": [],
        "partial_unexpected_index_list": []
      },
      "meta": {},
      "exception_info": {
        "raised_exception": false,
        "exception_traceback": null,
        "exception_message": null
      }
    },
    {
      "success": false,
      "expectation_config": {
        "type": "expect_column_values_to_be_between",
        "kwargs": {
          "batch_id": "dataSourceSample-dataAssetSample",
          "column": "passenger_count",
          "min_value": 0.0,
          "max_value": 4.0
        },
        "meta": {
          "expectation_name": "checkAcceptedValues",
          "data_product_name": "consuntiviDiProduzione"
        },
        "id": "66382b9d-3214-41d7-a182-30becda19fc1"
      },
      "result": {
        "element_count": 10000,
        "unexpected_count": 667,
        "unexpected_percent": 6.67,
        "partial_unexpected_list": [
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5
        ],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 6.67,
        "unexpected_percent_nonmissing": 6.67,
        "partial_unexpected_counts": [
          {
            "value": 5,
            "count": 20
          }
        ],
        "partial_unexpected_index_list": [
          9333,
          9334,
          9335,
          9336,
          9337,
          9338,
          9339,
          9340,
          9341,
          9342,
          9343,
          9344,
          9345,
          9346,
          9347,
          9348,
          9349,
          9350,
          9351,
          9352
        ]
      },
      "meta": {},
      "exception_info": {
        "raised_exception": false,
        "exception_traceback": null,
        "exception_message": null
      }
    },
    {
      "success": true,
      "expectation_config": {
        "type": "expect_column_values_to_match_regex",
        "kwargs": {
          "batch_id": "dataSourceSample-dataAssetSample",
          "column": "pickup_datetime",
          "regex": "^\\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])\\s([01]\\d|2[0-3]):([0-5]\\d):([0-5]\\d)$"
        },
        "meta": {
          "expectation_name": "checkAcceptedValues",
          "data_product_name": "consuntiviDiProduzione"
        },
        "id": "cae5c5d5-c019-4508-a6b4-ab80fc0d62c3"
      },
      "result": {
        "element_count": 10000,
        "unexpected_count": 0,
        "unexpected_percent": 0.0,
        "partial_unexpected_list": [],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 0.0,
        "unexpected_percent_nonmissing": 0.0,
        "partial_unexpected_counts": [],
        "partial_unexpected_index_list": []
      },
      "meta": {},
      "exception_info": {
        "raised_exception": false,
        "exception_traceback": null,
        "exception_message": null
      }
    }
  ],
  "suite_name": "dataSourceSample-dataAssetSample",
  "suite_parameters": {},
  "statistics": {
    "evaluated_expectations": 3,
    "successful_expectations": 2,
    "unsuccessful_expectations": 1,
    "success_percent": 66.66666666666666
  },
  "meta": {
    "great_expectations_version": "1.3.0",
    "batch_spec": {
      "batch_data": "PandasDataFrame"
    },
    "batch_markers": {
      "ge_load_time": "20250128T095853.440943Z",
      "pandas_data_fingerprint": "c4f929e6d4fab001fedc9e075bf4b612"
    },
    "active_batch_definition": {
      "datasource_name": "dataSourceSample",
      "data_connector_name": "fluent",
      "data_asset_name": "dataAssetSample",
      "batch_identifiers": {
        "dataframe": "<DATAFRAME>"
      }
    },
    "validation_id": "35b060ac-db19-4531-a6e1-b402b0f60f24",
    "checkpoint_id": null,
    "batch_parameters": {
      "dataframe": "<DATAFRAME>"
    }
  },
  "id": null
}
{
  "success": false,
  "results": [
    {
      "success": true,
      "expectation_config": {
        "type": "expect_column_values_to_not_be_null",
        "kwargs": {
          "batch_id": "dataSourceSample-dataAssetSample",
          "column": "vendor_id"
        },
        "meta": {
          "expectation_name": "checkNotNull",
          "data_product_name": "consuntiviDiProduzione"
        },
        "id": "d3243727-a938-4b5a-b24a-ac8a6a993440"
      },
      "result": {
        "element_count": 10000,
        "unexpected_count": 0,
        "unexpected_percent": 0.0,
        "partial_unexpected_list": [],
        "partial_unexpected_counts": [],
        "partial_unexpected_index_list": []
      },
      "meta": {},
      "exception_info": {
        "raised_exception": false,
        "exception_traceback": null,
        "exception_message": null
      }
    },
    {
      "success": false,
      "expectation_config": {
        "type": "expect_column_values_to_be_between",
        "kwargs": {
          "batch_id": "dataSourceSample-dataAssetSample",
          "column": "passenger_count",
          "min_value": 0.0,
          "max_value": 4.0
        },
        "meta": {
          "expectation_name": "checkAcceptedValues",
          "data_product_name": "consuntiviDiProduzione"
        },
        "id": "66382b9d-3214-41d7-a182-30becda19fc1"
      },
      "result": {
        "element_count": 10000,
        "unexpected_count": 667,
        "unexpected_percent": 6.67,
        "partial_unexpected_list": [
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5,
          5
        ],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 6.67,
        "unexpected_percent_nonmissing": 6.67,
        "partial_unexpected_counts": [
          {
            "value": 5,
            "count": 20
          }
        ],
        "partial_unexpected_index_list": [
          9333,
          9334,
          9335,
          9336,
          9337,
          9338,
          9339,
          9340,
          9341,
          9342,
          9343,
          9344,
          9345,
          9346,
          9347,
          9348,
          9349,
          9350,
          9351,
          9352
        ]
      },
      "meta": {},
      "exception_info": {
        "raised_exception": false,
        "exception_traceback": null,
        "exception_message": null
      }
    },
    {
      "success": true,
      "expectation_config": {
        "type": "expect_column_values_to_match_regex",
        "kwargs": {
          "batch_id": "dataSourceSample-dataAssetSample",
          "column": "pickup_datetime",
          "regex": "^\\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])\\s([01]\\d|2[0-3]):([0-5]\\d):([0-5]\\d)$"
        },
        "meta": {
          "expectation_name": "checkAcceptedValues",
          "data_product_name": "consuntiviDiProduzione"
        },
        "id": "cae5c5d5-c019-4508-a6b4-ab80fc0d62c3"
      },
      "result": {
        "element_count": 10000,
        "unexpected_count": 0,
        "unexpected_percent": 0.0,
        "partial_unexpected_list": [],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 0.0,
        "unexpected_percent_nonmissing": 0.0,
        "partial_unexpected_counts": [],
        "partial_unexpected_index_list": []
      },
      "meta": {},
      "exception_info": {
        "raised_exception": false,
        "exception_traceback": null,
        "exception_message": null
      }
    }
  ],
  "suite_name": "dataSourceSample-dataAssetSample",
  "suite_parameters": {},
  "statistics": {
    "evaluated_expectations": 3,
    "successful_expectations": 2,
    "unsuccessful_expectations": 1,
    "success_percent": 66.66666666666666
  },
  "meta": {
    "great_expectations_version": "1.3.0",
    "batch_spec": {
      "batch_data": "PandasDataFrame"
    },
    "batch_markers": {
      "ge_load_time": "20250128T095853.702507Z",
      "pandas_data_fingerprint": "c4f929e6d4fab001fedc9e075bf4b612"
    },
    "active_batch_definition": {
      "datasource_name": "dataSourceSample",
      "data_connector_name": "fluent",
      "data_asset_name": "dataAssetSample",
      "batch_identifiers": {
        "dataframe": "<DATAFRAME>"
      }
    },
    "validation_id": "35b060ac-db19-4531-a6e1-b402b0f60f24",
    "checkpoint_id": null,
    "batch_parameters": {
      "dataframe": "<DATAFRAME>"
    }
  },
  "id": null
}
```

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
    "resource_metrics": [
        {
            "resource": {
                "attributes": {
                    "telemetry.sdk.language": "python",
                    "telemetry.sdk.name": "opentelemetry",
                    "telemetry.sdk.version": "1.29.0",
                    "service.name": "consuntiviDiProduzione-quality_sidecar",
                    "telemetry.auto.version": "0.50b0"
                },
                "schema_url": ""
            },
            "scope_metrics": [
                {
                    "scope": {
                        "name": "__main__",
                        "version": "",
                        "schema_url": "",
                        "attributes": null
                    },
                    "metrics": [
                        {
                            "name": "datasourcesample-dataassetsample",
                            "description": "Validation results for suite: dataSourceSample-dataAssetSample",
                            "unit": "%",
                            "data": {
                                "data_points": [
                                    {
                                        "attributes": {
                                            "signal_type": "DATA_QUALITY",
                                            "element_count": 10000,
                                            "unexpected_count": 0,
                                            "expectation_name": "checkNotNull",
                                            "data_product_name": "consuntiviDiProduzione",
                                            "suite_name": "dataSourceSample-dataAssetSample",
                                            "data_source_name": "dataSourceSample",
                                            "data_asset_name": "dataAssetSample",
                                            "column_name": "vendor_id"
                                        },
                                        "start_time_unix_nano": null,
                                        "time_unix_nano": 1738058333992842935,
                                        "value": 100.0,
                                        "exemplars": []
                                    },
                                    {
                                        "attributes": {
                                            "signal_type": "DATA_QUALITY",
                                            "element_count": 10000,
                                            "unexpected_count": 667,
                                            "expectation_name": "checkAcceptedValues",
                                            "data_product_name": "consuntiviDiProduzione",
                                            "suite_name": "dataSourceSample-dataAssetSample",
                                            "data_source_name": "dataSourceSample",
                                            "data_asset_name": "dataAssetSample",
                                            "column_name": "passenger_count"
                                        },
                                        "start_time_unix_nano": null,
                                        "time_unix_nano": 1738058333992842935,
                                        "value": 93.33,
                                        "exemplars": []
                                    },
                                    {
                                        "attributes": {
                                            "signal_type": "DATA_QUALITY",
                                            "element_count": 10000,
                                            "unexpected_count": 0,
                                            "expectation_name": "checkAcceptedValues",
                                            "data_product_name": "consuntiviDiProduzione",
                                            "suite_name": "dataSourceSample-dataAssetSample",
                                            "data_source_name": "dataSourceSample",
                                            "data_asset_name": "dataAssetSample",
                                            "column_name": "pickup_datetime"
                                        },
                                        "start_time_unix_nano": null,
                                        "time_unix_nano": 1738058333992842935,
                                        "value": 100.0,
                                        "exemplars": []
                                    }
                                ]
                            }
                        }
                    ],
                    "schema_url": ""
                }
            ],
            "schema_url": ""
        }
    ]
}
```

## Configurazione
Di seguito vengono descritte le variabili di ambiente necessarie per il corretto funzionamento dell'applicazione e definite nel ``Dockerfile``.

**NB**: le variabili descritte di seguito vengono sovrascritte da quelle usate per configurare il sidecar all'interno del `docker-compose.yml`, e definite nel file `.env` per semplificare la gestione e la modifica delle configurazioni. Entrambi i file sono definiti a livello di root del progetto ```quality```.

### Dettaglio delle Variabili    

1. **`OTEL_METRIC_EXPORT_INTERVAL`**: Intervallo in millisecondi per l'esportazione delle metriche OTLP (default: 60 secondi).
    ```env
    ENV OTEL_METRIC_EXPORT_INTERVAL=10000
    ```

2. **`DATA_PRODUCT_NAME`**: Nome del data product.
    ```env
    ENV DATA_PRODUCT_NAME=dataProductNameSample
    ```

3. **`OTEL_SERVICE_NAME`**: Nome identificativo del servizio OTLP.
    ```env
    ENV OTEL_SERVICE_NAME=${DATA_PRODUCT_NAME}-quality_sidecar
    ```

4. **`EXPECTATIONS_JSON_FILE_PATH`**: Percorso del file JSON contenente le configurazioni per Great Expectations ([vai alla sezione specifica](#file-di-configurazione)).  
   ```env
    ENV EXPECTATIONS_JSON_FILE_PATH=resources/gx_v0.1.json
    ```

### Dettaglio delle Dockerfile

Alla fine del Dockerfile è presente il comando che lancia l'applicazione:
```
CMD ["sh", "-c", "opentelemetry-instrument --metrics_exporter otlp,console --logs_exporter otlp,console python build/app.py $EXPECTATIONS_JSON_FILE_PATH"]
```
L'applicazione riceve quindi in input il percorso del file json contenente le configurazioni per Great Expectations.

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
L'applicazione genererà quindi metriche in formato OTLP.

**NB**: L'esecuzione stand-alone dell'applicazione è sconsigliata, poiché è progettata per funzionare insieme a un collector che riceva le metriche generate (nei log si potrà notare che l'applicazione tenterà sempre di connettersi al collector di default `localhost:4317`).  L'esecuzione dell'applicazione in modo indipendente può essere utile per testare i singoli test o per verificare il formato delle metriche generate, ma senza un collector attivo (lanciato con il docker-compose.yml di progetto), la generazione delle metriche non ha alcun effetto pratico.

### Test
L'applicazione viene testata prima di ogni esecuzione grazie al comando all'interno del `Dockerfile`:
```
RUN pytest -v /app/tests --maxfail=1 --disable-warnings -s
```

I test sono stati creati mediante la libreria `pytest` al fine di testare le sole funzionalità di Great Expectations.