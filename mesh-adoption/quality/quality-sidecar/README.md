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

### File di Configurazione
I file all'interno della cartella **/resources** contengono le configurazioni necessarie per la corretta esecuzione di **Great Expectations** all'interno del sidecar, in particolare le aspettative di validazione sui dati. Ogni file di configurazione definisce tutti i controlli per l'intero data product.

**Nota**: per convenzione, i file di configurazione vengono nominati includendo la versione, ad esempio:
gx_v<i>MAJOR.MINOR</i>.json

#### Struttura del file JSON
Il file di configurazione permette di definire tutti i controlli di data quality per l'intero data product. Contiene una lista di elementi che sono rappresentativi per un sistema fisico: ogni elemento contiene poi i riferimenti al sistema e un insieme di aspettative. Nel dettaglio, ogni elemento della lista presenta le seguenti informazioni:

- `system_name`: il nome del sistema fisico
- `system_type`: la tipologia del sistema. Il quality sidecar al suo interno include e gestisce una serie di **connettori**: essi hanno il compito di estrapolare il Dataframe Pandas, da dare in pasto a GX, in modalita' differenti a seconda di quale sia il tipo di sorgente. Attualmente vengono gestiti i seguenti tipi di connettori:

  - **UNITY**: estrapola i dati da Unity Catalog
  - **CSV**: estrapola i dati da un file CSV (il cui percorso viene specificato nel file di configurazione)

- `expectations`: contiene la lista di controlli da effettuare su tabelle e campi del sistema. Nel dettaglio, ogni elemento contiene le seguenti informazioni:
    - `check_name`: il riferimento al quality check presente su Blindata
    - `asset_name`: il nome dell'asset dati
    - `asset_kwargs`: i parametri (non necessari) per l'asset, come nel caso di connettore CSV il path del file
    - `expectation_type`: il tipo di aspettativa da applicare (ad esempio, verifica che un valore non sia nullo, che un valore sia compreso tra due estremi, o che un valore corrisponda a una regex).

        - Consultare la gallery di Expectations ufficiale: https://greatexpectations.io/expectations/
    - `kwargs`: i parametri necessari per eseguire l'aspettativa (ad esempio, il nome della colonna da verificare o i valori di limite per un intervallo).

      - Consultare la gallery di Expectations ufficiale: https://greatexpectations.io/expectations/


Di seguito un esempio di file di configurazione per `"system_type": "UNITY"`:  
```json
[
    {
        "system_name": "dit_dicox_dpflab_dds2_dev",
        "system_type": "UNITY",
        "expectations": [
            {
                "check_name": "sapIess-dit_dicox_dpflab_dds2_dev-ddsdltdb.segnogiornalieroquartoorario-PK-ExpectColumnValuesToNotBeNull",
                "expectation_type": "ExpectColumnValuesToNotBeNull",
                "asset_name": "ddsdltdb.segnogiornalieroquartoorario",
                "kwargs": {
                    "column": "PK"
                }
            },
            {
                "check_name": "sapIess-dit_dicox_dpflab_dds2_dev-ddsdltdb.segnogiornalieroquartoorario-Macrozona-ExpectColumnValuesToBeInSet",
                "expectation_type": "ExpectColumnValuesToBeInSet",
                "asset_name": "ddsdltdb.segnogiornalieroquartoorario",
                "kwargs": {
                    "column": "Macrozona",
                    "value_set": ["NORD", "SUD"]
                }
            },
            {
                "check_name": "sapIess-dit_dicox_dpflab_dds2_dev.ddsdltdb.segnogiornalieroquartoorario-ScambiMWh-ExpectColumnValuesToBeBetween",
                "expectation_type": "ExpectColumnValuesToBeBetween",
                "asset_name": "ddsdltdb.segnogiornalieroquartoorario",
                "kwargs": {
                    "column": "Scambi[MWh]",
                    "min_value": -930,
                    "max_value": 800
                }
            },
            {
                "check_name": "sapIess-dit_dicox_dpflab_dds2_dev.ddsdltdb.segnogiornalieroquartoorario-DataDiRiferimento-ExpectColumnValuesToMatchRegex",
                "expectation_type": "ExpectColumnValuesToMatchRegex",
                "asset_name": "ddsdltdb.segnogiornalieroquartoorario",
                "kwargs": {
                    "column": "DataDiRiferimento",
                    "regex": "^\\d{2}/\\d{2}/\\d{4}$"
                }
            },
            {
                "check_name": "sapIess-PrezzoSbilanciamento-PrezzoBase-ExpectColumnPairValuesAToBeGreaterThanB",
                "expectation_type": "ExpectColumnPairValuesAToBeGreaterThanB",
                "asset_name": "ddsdltdb.prezzigiornalieriquartoorari",
                "kwargs": {
                    "column_A": "PrezzoSbilanciamento",
                    "column_B": "PrezzoBase",
                    "or_equal": true
                }
            },
            {
                "check_name": "sapIess-ComponenteIncentivante-ExpectColumnValuesToBeBetween",
                "expectation_type": "ExpectColumnValuesToBeBetween",
                "asset_name": "ddsdltdb.prezzigiornalieriquartoorari",
                "kwargs": {
                    "column": "ComponenteIncentivante",
                    "min_value": 0
                }
            }
        ]
    }
]
```

Di seguito un esempio di file di configurazione per `"system_type": "CSV"`:
```json
[
    {
        "system_name": "dit_dicox_dpflab_dds2_dev",
        "system_type": "CSV",
        "expectations": [
            {
                "check_name": "sapNac-dit_dicox_dpflab_dds2_dev-ddsdltdb.segnogiornalieroquartoorario-PK-ExpectColumnValuesToNotBeNull",
                "expectation_type": "ExpectColumnValuesToNotBeNull",
                "asset_name": "ddsdltdb.segnogiornalieroquartoorario",
                "asset_kwargs": {
                    "path": "resources/sapNac/sapNac-segnogiornalieroquartoorario.csv"
                },
                "kwargs": {
                    "column": "PK"
                }
            },
            {
                "check_name": "sapNac-dit_dicox_dpflab_dds2_dev-ddsdltdb.segnogiornalieroquartoorario-Macrozona-ExpectColumnValuesToBeInSet",
                "expectation_type": "ExpectColumnValuesToBeInSet",
                "asset_name": "ddsdltdb.segnogiornalieroquartoorario",
                "asset_kwargs": {
                    "path": "resources/sapNac/sapNac-segnogiornalieroquartoorario.csv"
                },
                "kwargs": {
                    "column": "Macrozona",
                    "value_set": ["NORD", "SUD"]
                }
            },
            {
                "check_name": "sapNac-dit_dicox_dpflab_dds2_dev.ddsdltdb.segnogiornalieroquartoorario-ScambiMWh-ExpectColumnValuesToBeBetween",
                "expectation_type": "ExpectColumnValuesToBeBetween",
                "asset_name": "ddsdltdb.segnogiornalieroquartoorario",
                "asset_kwargs": {
                    "path": "resources/sapNac/sapNac-segnogiornalieroquartoorario.csv"
                },
                "kwargs": {
                    "column": "Scambi[MWh]",
                    "min_value": -930,
                    "max_value": 800
                }
            },
            {
                "check_name": "sapNac-dit_dicox_dpflab_dds2_dev.ddsdltdb.segnogiornalieroquartoorario-DataDiRiferimento-ExpectColumnValuesToMatchRegex",
                "expectation_type": "ExpectColumnValuesToMatchRegex",
                "asset_name": "ddsdltdb.segnogiornalieroquartoorario",
                "asset_kwargs": {
                    "path": "resources/sapNac/sapNac-segnogiornalieroquartoorario.csv"
                },
                "kwargs": {
                    "column": "DataDiRiferimento",
                    "regex": "^\\d{2}/\\d{2}/\\d{4}$"
                }
            },
            {
                "check_name": "sapNac-PrezzoSbilanciamento-PrezzoBase-ExpectColumnPairValuesAToBeGreaterThanB",
                "expectation_type": "ExpectColumnPairValuesAToBeGreaterThanB",
                "asset_name": "ddsdltdb.prezzigiornalieriquartoorari",
                "asset_kwargs": {
                    "path": "resources/sapNac/sapNac-prezzigiornalieriquartoorari.csv"
                },
                "kwargs": {
                    "column_A": "PrezzoSbilanciamento",
                    "column_B": "PrezzoBase",
                    "or_equal": true
                }
            },
            {
                "check_name": "sapNac-ComponenteIncentivante-ExpectColumnValuesToBeBetween",
                "expectation_type": "ExpectColumnValuesToBeBetween",
                "asset_name": "ddsdltdb.prezzigiornalieriquartoorari",
                "asset_kwargs": {
                    "path": "resources/sapNac/sapNac-prezzigiornalieriquartoorari.csv"
                },
                "kwargs": {
                    "column": "ComponenteIncentivante",
                    "min_value": 0
                }
            }
        ]
    }
]
```

### Great Expectations
Il modulo di **Great Expectations** prende in input il file di configurazione e restituisce in output i risultati delle validazione.

Di seguito un esempio, con result_format="SUMMARY" (verbosità di default):
```json
[
  {
    "success":true,
    "expectation_config":{
      "type":"expect_column_values_to_not_be_null",
      "kwargs":{
        "batch_id":"dit_dicox_dpflab_dds2_dev-ddsdltdb.segnogiornalieroquartoorario",
        "column":"PK"
      },
      "meta":{
        "check_name":"sapIess-dit_dicox_dpflab_dds2_dev-ddsdltdb.segnogiornalieroquartoorario-PK-ExpectColumnValuesToNotBeNull",
        "data_product_name":"sapIess"
      }
    },
    "result":{
      "element_count":31480,
      "unexpected_count":0,
      "unexpected_percent":0.0,
      "partial_unexpected_list":[
        
      ],
      "partial_unexpected_counts":[
        
      ],
      "partial_unexpected_index_list":[
        
      ]
    },
    "meta":{
      
    },
    "exception_info":{
      "raised_exception":false,
      "exception_traceback":"None",
      "exception_message":"None"
    }
  },
  {
    "success":true,
    "expectation_config":{
      "type":"expect_column_values_to_be_in_set",
      "kwargs":{
        "batch_id":"dit_dicox_dpflab_dds2_dev-ddsdltdb.segnogiornalieroquartoorario",
        "column":"Macrozona",
        "value_set":[
          "NORD",
          "SUD"
        ]
      },
      "meta":{
        "check_name":"sapIess-dit_dicox_dpflab_dds2_dev-ddsdltdb.segnogiornalieroquartoorario-Macrozona-ExpectColumnValuesToBeInSet",
        "data_product_name":"sapIess"
      }
    },
    "result":{
      "element_count":31480,
      "unexpected_count":0,
      "unexpected_percent":0.0,
      "partial_unexpected_list":[
        
      ],
      "missing_count":0,
      "missing_percent":0.0,
      "unexpected_percent_total":0.0,
      "unexpected_percent_nonmissing":0.0,
      "partial_unexpected_counts":[
        
      ],
      "partial_unexpected_index_list":[
        
      ]
    },
    "meta":{
      
    },
    "exception_info":{
      "raised_exception":false,
      "exception_traceback":"None",
      "exception_message":"None"
    }
  },
  {
    "success":false,
    "expectation_config":{
      "type":"expect_column_values_to_be_between",
      "kwargs":{
        "batch_id":"dit_dicox_dpflab_dds2_dev-ddsdltdb.segnogiornalieroquartoorario",
        "column":"Scambi[MWh]",
        "min_value":-930.0,
        "max_value":800.0
      },
      "meta":{
        "check_name":"sapIess-dit_dicox_dpflab_dds2_dev.ddsdltdb.segnogiornalieroquartoorario-ScambiMWh-ExpectColumnValuesToBeBetween",
        "data_product_name":"sapIess"
      }
    },
    "result":{
      "element_count":31480,
      "unexpected_count":1207,
      "unexpected_percent":3.83418043202033,
      "partial_unexpected_list":[
        -931.089,
        -949.352,
        -947.092,
        -931.142,
        931.089,
        916.631,
        861.242,
        904.996,
        947.092,
        958.446,
        931.142,
        854.271,
        -971.97,
        882.702,
        904.92,
        827.641,
        971.97,
        914.404,
        941.671,
        898.177
      ],
      "missing_count":0,
      "missing_percent":0.0,
      "unexpected_percent_total":3.83418043202033,
      "unexpected_percent_nonmissing":3.83418043202033,
      "partial_unexpected_counts":[
        {
          "value":-971.97,
          "count":1
        },
        {
          "value":-949.352,
          "count":1
        },
        {
          "value":-947.092,
          "count":1
        },
        {
          "value":-931.142,
          "count":1
        },
        {
          "value":-931.089,
          "count":1
        },
        {
          "value":827.641,
          "count":1
        },
        {
          "value":854.271,
          "count":1
        },
        {
          "value":861.242,
          "count":1
        },
        {
          "value":882.702,
          "count":1
        },
        {
          "value":898.177,
          "count":1
        },
        {
          "value":904.92,
          "count":1
        },
        {
          "value":904.996,
          "count":1
        },
        {
          "value":914.404,
          "count":1
        },
        {
          "value":916.631,
          "count":1
        },
        {
          "value":931.089,
          "count":1
        },
        {
          "value":931.142,
          "count":1
        },
        {
          "value":941.671,
          "count":1
        },
        {
          "value":947.092,
          "count":1
        },
        {
          "value":958.446,
          "count":1
        },
        {
          "value":971.97,
          "count":1
        }
      ],
      "partial_unexpected_index_list":[
        0,
        3,
        8,
        10,
        50,
        59,
        71,
        75,
        78,
        79,
        84,
        85,
        89,
        91,
        97,
        108,
        115,
        119,
        128,
        130
      ]
    },
    "meta":{
      
    },
    "exception_info":{
      "raised_exception":false,
      "exception_traceback":"None",
      "exception_message":"None"
    }
  },
  {
    "success":false,
    "expectation_config":{
      "type":"expect_column_values_to_match_regex",
      "kwargs":{
        "batch_id":"dit_dicox_dpflab_dds2_dev-ddsdltdb.segnogiornalieroquartoorario",
        "column":"DataDiRiferimento",
        "regex":"^\\d{2}/\\d{2}/\\d{4}$"
      },
      "meta":{
        "check_name":"sapIess-dit_dicox_dpflab_dds2_dev.ddsdltdb.segnogiornalieroquartoorario-DataDiRiferimento-ExpectColumnValuesToMatchRegex",
        "data_product_name":"sapIess"
      }
    },
    "result":{
      "element_count":31480,
      "unexpected_count":31480,
      "unexpected_percent":100.0,
      "partial_unexpected_list":[
        "29/01/2024 18:45",
        "29/01/2024 09:00",
        "29/01/2024 14:30",
        "29/01/2024 19:30",
        "29/01/2024 08:15",
        "29/01/2024 11:45",
        "29/01/2024 14:00",
        "29/01/2024 10:15",
        "29/01/2024 19:45",
        "29/01/2024 03:45",
        "29/01/2024 20:30",
        "29/01/2024 14:45",
        "29/01/2024 07:45",
        "29/01/2024 04:45",
        "29/01/2024 14:15",
        "29/01/2024 03:15",
        "29/01/2024 01:45",
        "29/01/2024 00:45",
        "29/01/2024 06:00",
        "29/01/2024 22:45"
      ],
      "missing_count":0,
      "missing_percent":0.0,
      "unexpected_percent_total":100.0,
      "unexpected_percent_nonmissing":100.0,
      "partial_unexpected_counts":[
        {
          "value":"29/01/2024 00:45",
          "count":1
        },
        {
          "value":"29/01/2024 01:45",
          "count":1
        },
        {
          "value":"29/01/2024 03:15",
          "count":1
        },
        {
          "value":"29/01/2024 03:45",
          "count":1
        },
        {
          "value":"29/01/2024 04:45",
          "count":1
        },
        {
          "value":"29/01/2024 06:00",
          "count":1
        },
        {
          "value":"29/01/2024 07:45",
          "count":1
        },
        {
          "value":"29/01/2024 08:15",
          "count":1
        },
        {
          "value":"29/01/2024 09:00",
          "count":1
        },
        {
          "value":"29/01/2024 10:15",
          "count":1
        },
        {
          "value":"29/01/2024 11:45",
          "count":1
        },
        {
          "value":"29/01/2024 14:00",
          "count":1
        },
        {
          "value":"29/01/2024 14:15",
          "count":1
        },
        {
          "value":"29/01/2024 14:30",
          "count":1
        },
        {
          "value":"29/01/2024 14:45",
          "count":1
        },
        {
          "value":"29/01/2024 18:45",
          "count":1
        },
        {
          "value":"29/01/2024 19:30",
          "count":1
        },
        {
          "value":"29/01/2024 19:45",
          "count":1
        },
        {
          "value":"29/01/2024 20:30",
          "count":1
        },
        {
          "value":"29/01/2024 22:45",
          "count":1
        }
      ],
      "partial_unexpected_index_list":[
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        19
      ]
    },
    "meta":{
      
    },
    "exception_info":{
      "raised_exception":false,
      "exception_traceback":"None",
      "exception_message":"None"
    }
  },
  {
    "success":false,
    "expectation_config":{
      "type":"expect_column_pair_values_a_to_be_greater_than_b",
      "kwargs":{
        "batch_id":"dit_dicox_dpflab_dds2_dev-ddsdltdb.prezzigiornalieriquartoorari",
        "column_A":"PrezzoSbilanciamento",
        "column_B":"PrezzoBase",
        "or_equal":true
      },
      "meta":{
        "check_name":"sapIess-PrezzoSbilanciamento-PrezzoBase-ExpectColumnPairValuesAToBeGreaterThanB",
        "data_product_name":"sapIess"
      }
    },
    "result":{
      "element_count":32959,
      "unexpected_count":1323,
      "unexpected_percent":4.014078097029643,
      "partial_unexpected_list":[
        [
          115.0,
          161.793
        ],
        [
          110.0,
          123.837
        ],
        [
          99.82,
          104.457
        ],
        [
          76.5,
          99.438
        ],
        [
          59.213,
          91.946
        ],
        [
          100.05,
          102.15
        ],
        [
          96.2,
          99.521
        ],
        [
          135.0,
          136.5
        ],
        [
          83.19,
          104.019
        ],
        [
          100.96,
          126.558
        ],
        [
          59.75,
          83.295
        ],
        [
          95.19,
          105.714
        ],
        [
          78.5,
          87.356
        ],
        [
          65.0,
          72.708
        ],
        [
          80.5,
          86.147
        ],
        [
          48.12,
          87.39
        ],
        [
          112.0,
          134.87
        ],
        [
          78.7,
          80.042
        ],
        [
          86.07,
          102.374
        ],
        [
          105.0,
          105.698
        ]
      ],
      "missing_count":0,
      "missing_percent":0.0,
      "unexpected_percent_total":4.014078097029643,
      "unexpected_percent_nonmissing":4.014078097029643,
      "partial_unexpected_counts":[
        {
          "value":[
            48.12,
            87.39
          ],
          "count":1
        },
        {
          "value":[
            59.213,
            91.946
          ],
          "count":1
        },
        {
          "value":[
            59.75,
            83.295
          ],
          "count":1
        },
        {
          "value":[
            65.0,
            72.708
          ],
          "count":1
        },
        {
          "value":[
            76.5,
            99.438
          ],
          "count":1
        },
        {
          "value":[
            78.5,
            87.356
          ],
          "count":1
        },
        {
          "value":[
            78.7,
            80.042
          ],
          "count":1
        },
        {
          "value":[
            80.5,
            86.147
          ],
          "count":1
        },
        {
          "value":[
            83.19,
            104.019
          ],
          "count":1
        },
        {
          "value":[
            86.07,
            102.374
          ],
          "count":1
        },
        {
          "value":[
            95.19,
            105.714
          ],
          "count":1
        },
        {
          "value":[
            96.2,
            99.521
          ],
          "count":1
        },
        {
          "value":[
            99.82,
            104.457
          ],
          "count":1
        },
        {
          "value":[
            100.05,
            102.15
          ],
          "count":1
        },
        {
          "value":[
            100.96,
            126.558
          ],
          "count":1
        },
        {
          "value":[
            105.0,
            105.698
          ],
          "count":1
        },
        {
          "value":[
            110.0,
            123.837
          ],
          "count":1
        },
        {
          "value":[
            112.0,
            134.87
          ],
          "count":1
        },
        {
          "value":[
            115.0,
            161.793
          ],
          "count":1
        },
        {
          "value":[
            135.0,
            136.5
          ],
          "count":1
        }
      ],
      "partial_unexpected_index_list":[
        20,
        32,
        44,
        66,
        78,
        138,
        249,
        345,
        352,
        399,
        412,
        525,
        578,
        615,
        646,
        723,
        739,
        782,
        810,
        871
      ]
    },
    "meta":{
      
    },
    "exception_info":{
      "raised_exception":false,
      "exception_traceback":"None",
      "exception_message":"None"
    }
  },
  {
    "success":false,
    "expectation_config":{
      "type":"expect_column_values_to_be_between",
      "kwargs":{
        "batch_id":"dit_dicox_dpflab_dds2_dev-ddsdltdb.prezzigiornalieriquartoorari",
        "column":"ComponenteIncentivante",
        "min_value":0.0
      },
      "meta":{
        "check_name":"sapIess-ComponenteIncentivante-ExpectColumnValuesToBeBetween",
        "data_product_name":"sapIess"
      }
    },
    "result":{
      "element_count":32959,
      "unexpected_count":1322,
      "unexpected_percent":4.011044024393944,
      "partial_unexpected_list":[
        -46.793,
        -13.837,
        -4.637,
        -22.938,
        -32.733,
        -2.1,
        -3.321,
        -1.5,
        -20.829,
        -25.598,
        -23.544,
        -10.524,
        -8.856,
        -7.708,
        -5.647,
        -39.27,
        -22.87,
        -1.342,
        -16.304,
        -0.698
      ],
      "missing_count":0,
      "missing_percent":0.0,
      "unexpected_percent_total":4.011044024393944,
      "unexpected_percent_nonmissing":4.011044024393944,
      "partial_unexpected_counts":[
        {
          "value":-46.793,
          "count":1
        },
        {
          "value":-39.27,
          "count":1
        },
        {
          "value":-32.733,
          "count":1
        },
        {
          "value":-25.598,
          "count":1
        },
        {
          "value":-23.544,
          "count":1
        },
        {
          "value":-22.938,
          "count":1
        },
        {
          "value":-22.87,
          "count":1
        },
        {
          "value":-20.829,
          "count":1
        },
        {
          "value":-16.304,
          "count":1
        },
        {
          "value":-13.837,
          "count":1
        },
        {
          "value":-10.524,
          "count":1
        },
        {
          "value":-8.856,
          "count":1
        },
        {
          "value":-7.708,
          "count":1
        },
        {
          "value":-5.647,
          "count":1
        },
        {
          "value":-4.637,
          "count":1
        },
        {
          "value":-3.321,
          "count":1
        },
        {
          "value":-2.1,
          "count":1
        },
        {
          "value":-1.5,
          "count":1
        },
        {
          "value":-1.342,
          "count":1
        },
        {
          "value":-0.698,
          "count":1
        }
      ],
      "partial_unexpected_index_list":[
        20,
        32,
        44,
        66,
        78,
        138,
        249,
        345,
        352,
        399,
        412,
        525,
        578,
        615,
        646,
        723,
        739,
        782,
        810,
        871
      ]
    },
    "meta":{
      
    },
    "exception_info":{
      "raised_exception":false,
      "exception_traceback":"None",
      "exception_message":"None"
    }
  }
]
```

### OpenTelemetry

Il modulo di **Opentelemetry** viene utilizzato per raccogliere e inviare metriche, relative alla validazione dei dati con Great Expectations, tramite il protocollo OTLP a un sistema di monitoraggio esterno (es: Collector di piattaforma).

Di seguito un esempio di risultato di validazione con Great Expectations esportato come metrica tramite OTLP:
``` json
{
    "resource_metrics": [
        {
            "resource": {
                "attributes": {
                    "telemetry.sdk.language": "python",
                    "telemetry.sdk.name": "opentelemetry",
                    "telemetry.sdk.version": "1.30.0",
                    "service.name": "sapIess-quality_sidecar",
                    "telemetry.auto.version": "0.51b0"
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
                            "name": "sapiess",
                            "description": "",
                            "unit": "%",
                            "data": {
                                "data_points": [
                                    {
                                        "attributes": {
                                            "signal_type": "DATA_QUALITY",
                                            "checked_elements_nbr": 31480,
                                            "errors_nbr": 0,
                                            "check_name": "sapIess-dit_dicox_dpflab_dds2_dev-ddsdltdb.segnogiornalieroquartoorario-PK-ExpectColumnValuesToNotBeNull",
                                            "data_product_name": "sapIess"
                                        },
                                        "start_time_unix_nano": null,
                                        "time_unix_nano": 1739869072389701076,
                                        "value": 100.0,
                                        "exemplars": []
                                    },
                                    {
                                        "attributes": {
                                            "signal_type": "DATA_QUALITY",
                                            "checked_elements_nbr": 31480,
                                            "errors_nbr": 0,
                                            "check_name": "sapIess-dit_dicox_dpflab_dds2_dev-ddsdltdb.segnogiornalieroquartoorario-Macrozona-ExpectColumnValuesToBeInSet",
                                            "data_product_name": "sapIess"
                                        },
                                        "start_time_unix_nano": null,
                                        "time_unix_nano": 1739869072389701076,
                                        "value": 100.0,
                                        "exemplars": []
                                    },
                                    {
                                        "attributes": {
                                            "signal_type": "DATA_QUALITY",
                                            "checked_elements_nbr": 31480,
                                            "errors_nbr": 1207,
                                            "check_name": "sapIess-dit_dicox_dpflab_dds2_dev.ddsdltdb.segnogiornalieroquartoorario-ScambiMWh-ExpectColumnValuesToBeBetween",
                                            "data_product_name": "sapIess"
                                        },
                                        "start_time_unix_nano": null,
                                        "time_unix_nano": 1739869072389701076,
                                        "value": 96.16581956797967,
                                        "exemplars": []
                                    },
                                    {
                                        "attributes": {
                                            "signal_type": "DATA_QUALITY",
                                            "checked_elements_nbr": 31480,
                                            "errors_nbr": 31480,
                                            "check_name": "sapIess-dit_dicox_dpflab_dds2_dev.ddsdltdb.segnogiornalieroquartoorario-DataDiRiferimento-ExpectColumnValuesToMatchRegex",
                                            "data_product_name": "sapIess"
                                        },
                                        "start_time_unix_nano": null,
                                        "time_unix_nano": 1739869072389701076,
                                        "value": 0.0,
                                        "exemplars": []
                                    },
                                    {
                                        "attributes": {
                                            "signal_type": "DATA_QUALITY",
                                            "checked_elements_nbr": 32959,
                                            "errors_nbr": 1323,
                                            "check_name": "sapIess-PrezzoSbilanciamento-PrezzoBase-ExpectColumnPairValuesAToBeGreaterThanB",
                                            "data_product_name": "sapIess"
                                        },
                                        "start_time_unix_nano": null,
                                        "time_unix_nano": 1739869072389701076,
                                        "value": 95.98592190297036,
                                        "exemplars": []
                                    },
                                    {
                                        "attributes": {
                                            "signal_type": "DATA_QUALITY",
                                            "checked_elements_nbr": 32959,
                                            "errors_nbr": 1322,
                                            "check_name": "sapIess-ComponenteIncentivante-ExpectColumnValuesToBeBetween",
                                            "data_product_name": "sapIess"
                                        },
                                        "start_time_unix_nano": null,
                                        "time_unix_nano": 1739869072389701076,
                                        "value": 95.98895597560606,
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
Di seguito vengono descritte le variabili di ambiente necessarie per il corretto funzionamento dell'applicazione.

### Dettaglio delle Variabili    

1. **`DATA_PRODUCT_NAME`**: Nome del data product.
    ```env
    ENV DATA_PRODUCT_NAME=sapNac
    ```

2. **`OTEL_SERVICE_NAME`**: Nome identificativo del servizio OTLP.
    ```env
    ENV OTEL_SERVICE_NAME=${DATA_PRODUCT_NAME}-quality_sidecar
    ```

3. **`EXPECTATIONS_JSON_FILE_PATH`**: Percorso del file JSON contenente le configurazioni per Great Expectations ([vai alla sezione specifica](#file-di-configurazione)).  
   ```env
    ENV EXPECTATIONS_JSON_FILE_PATH=resources/sapNac/csv_v0.1.json
    ```

### Dettaglio delle Dockerfile

Alla fine del Dockerfile è presente il comando che lancia l'applicazione:
```
CMD ["python", "build/quality.py"]
```
Il file `quality.py` si occupa di richiamare i due moduli di GX e OTLP.

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