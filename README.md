# README

## Install

```bash
pip install invenio-analytics-importer
```


## Usage

**Retrieve analytics**

```bash
pipenv run invenio analytics_importer retrieve --from <YYYY-MM-DD> --to <YYYY-MM-DD> --output-dir <path>/<to>/<data>/
```


**Ingest**

```bash
pipenv run invenio analytics_importer ingest -f <stats file 1> -f <<stats file 2> ...
```
