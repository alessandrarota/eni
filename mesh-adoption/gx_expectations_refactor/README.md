# gx-expectations-refactor

## 📖 Table of Contents
1. [Definizione del Progetto](#definizione-del-progetto)
2. [Struttura del Progetto](#struttura-del-progetto)
3. [Getting Started](#getting-started)
    - [Prerequisiti](#prerequisiti)
    - [Installazione](#installazione)
    - [Esecuzione](#esecuzione)
4. [Dettagli Implementativi](#dettagli-implementativi)

## 📌 Definizione del Progetto

**gx_expectations_refactor** è un progetto Python per il refactoring dell'output delle expectations native di [Great Expectations](https://greatexpectations.io/gx-core/) e per la creazione di expectations custom non coperte nativamente dalla libreria.

<u>La versione di Great Expectations utilizzata è la **1.3.12**.</u>

## 📂 Struttura del Progetto

Il progetto è strutturato come segue:

- cartella **/tests**: contiene i test per testare i risultati delle expectations native e custom
- cartella **/support-scripts**: contiene degli script di supporto per analizzare le expectations e la documentazione del modulo ``core``
- cartella **/utils**: contiene la classe ``ResultFormatter.py`` per la gestione della formattazione dell'output di tutte le expectations (custom e native)
- cartella **/expectations**: contiene le classi delle expectations:
    - native (sovrascritte per modifica dell'output o recupero di ulteriori dati rispetto alle originali)
    - custom (ex-novo)
- cartella **/documentation**: contiene un excel con il dettaglio teorico dei controlli e delle modifiche/ggiunte effettuate

## 🚀 Getting Started

### ⚠️ Prerequisiti

Prima di avviare il progetto, assicurati di avere installato:

- **python>=3.11**
- **pip**

### 🔧 Installazione

1. **Clonare il Repository**
   ```bash
   git clone https://bitbucket.org/quantyca/gx-expectations-refactor/src/main/
   cd gx-expectations-refactor
   ```

2. **Installare le Dipendenze**
   ```bash
   pip install -r requirements.txt
   ```

### ▶️ Esecuzione

E' possibile testare le varie expectations mediante test:
```bash
pytest tests/test_refactor.py
```

## 💻 Dettagli Implementativi

### 📑 Formato dell'Output e Tipologia di Controlli

La classe `/utils/ResulFormatter.py` gestisce l'output per tutti i controlli:
```
"result": {
    "element_count": None,              # int or None
    "unexpected_count": None,           # int or None
    "unexpected_percent": None,         # float or None
    "expectation_output_metric": None   # float or boolean or None
  }
```
Di seguito il dettaglio dei vari campi:
- `element_count`: numero di righe totali su cui è stato eseguito il controllo
- `unexpected_count`: numero di righe che non hanno passato il controllo
- `unexpected_percent`: percentuale di righe che non hanno passato il controllo
- `expectation_output_metric`: valore assoluto/booleano esito del controllo (es: il valore massimo in una colonna)

A seconda del tipo di controllo, vengono valorizzati alcuni dei campi sopra. Distinguiamo 3 tipologie di controlli:
- con esiti **percentuali** (es: i valori in una colonna sono compresi in un range)
- con esiti **assoluti** (es: il valore massimo di una colonna è compreso in un range)
- con esiti **booleani** (es: le colonne di un tabella sono in un preciso ordine)

#### Percentuali
```
"result": {
    "element_count": 5,
    "unexpected_count": 4,
    "unexpected_percent": 80.0,
    "expectation_output_metric": null
  }
```
#### Assoluti
```
"result": {
    "element_count": 5,
    "unexpected_count": null,
    "unexpected_percent": null,
    "expectation_output_metric": 3.5
  }
```
#### Booleani
```
"result": {
    "element_count": 5,
    "unexpected_count": null,
    "unexpected_percent": null,
    "expectation_output_metric": false
  }
```
Non è detto che per tutti i controlli booleani venga valorizzato il campo `element_count`.

### 🛠️ Modifiche Applicate

Nella cartella `/expectations` sono presenti le classi delle expectations, suddivise in base alla gerarchia prevista dalla libreria.

Le classi relative ai controlli nativi della libreria mantengono il nome originale e, quando importate, sovrascrivono automaticamente l'implementazione predefinita.

In caso di modifiche ai controlli nativi (per adattare l'output o recuperare informazioni aggiuntive), è stata creata una copia della classe originale e successivamente è stata modificata, con l'aggiornamento o l'aggiunta, se assenti, dei seguenti metodi predefiniti di Great Expectations:
- `_get_validation_dependencies()`, per il recupero delle metriche necessarie
- `_validate()`, per la composizione dell'output, con eventuale logica di calcolo basata sulle metriche recuperate.

Il dettaglio delle classi coinvolte è presente nel file Excel all'interno della cartella `/documentation`.