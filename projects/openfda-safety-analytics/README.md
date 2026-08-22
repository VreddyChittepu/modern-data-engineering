# openFDA Safety Analytics

Incremental healthcare data engineering project using the openFDA APIs from `apis_for_data_engineering_projects.pdf`.

API used:

- openFDA API - Healthcare - free, optional API key for higher limits.
- Project idea - ingest drug, device and adverse-event data to build safety analytics use cases.

## Architecture

```text
openFDA REST API
      |
      v
Python incremental ingestion
      |
      v
Raw JSON landing
      |
      v
Delta Lake Bronze -> Silver -> Gold
```

## Included Endpoints

- Drug adverse events: `https://api.fda.gov/drug/event.json`
- Device adverse events: `https://api.fda.gov/device/event.json`
- Food adverse events: `https://api.fda.gov/food/event.json`

## Databricks Free Edition

Use this notebook:

```text
notebooks/serverless_free_edition_runner.py
```

It is designed for serverless compute. It calls openFDA when outbound access works and falls back to sample adverse-event records when the API is unreachable.

## Local Validation

```powershell
python -m compileall projects/openfda-safety-analytics/src projects/openfda-safety-analytics/tests
```

```powershell
$env:PYTHONPATH="projects/openfda-safety-analytics/src"
python -m pytest projects/openfda-safety-analytics/tests
```

## Data Layers

Bronze:

- raw JSON payload
- endpoint name
- ingestion timestamp
- source URL

Silver:

- event id
- received date
- report date
- serious flag
- patient reaction terms
- product names
- source endpoint

Gold:

- adverse-event counts by date and endpoint
- serious event counts
- top reaction terms
