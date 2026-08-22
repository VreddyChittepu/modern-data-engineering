# Streaming API Lakehouse

Streaming-focused data engineering project based on the API ideas from `apis_for_data_engineering_projects.pdf`.

The project intentionally uses only APIs from the PDF that fit real-time, event-feed, or streaming-style ingestion patterns:

- Wikimedia EventStreams - native server-sent event stream for live Wikimedia changes.
- MTA Real-Time API - transit feeds for delay and disruption use cases.
- Coinbase Exchange API - market trades and order-book style data.
- OpenSky Network API - periodically captured aircraft positions.
- USGS Earthquake API - incrementally discovered geospatial events.
- USGS Water Services - sensor-style river and streamflow measurements.
- openSenseMap API - environmental IoT sensor readings.
- GDELT API - continuously ingested global news events.
- Hacker News API - polling-based trending story/comment stream.

## Architecture

```text
Streaming API / Polling API
        |
        v
Python producer
        |
        v
Kafka topic: api.raw.events
        |
        v
Spark Structured Streaming
        |
        v
Delta Lake Bronze -> Silver -> Gold
```

## Project Layout

```text
configs/
  streaming_apis.json        API catalog and topic mapping
  spark.conf                 Spark defaults for local or Databricks jobs
src/streaming_api_lakehouse/
  api_catalog.py             Loads and validates the streaming API catalog
  producers/                 Wikimedia SSE and polling producer examples
  spark/                     Bronze, Silver and Gold Spark jobs
  utils/                     Shared HTTP helpers
tests/
  test_api_catalog.py        Lightweight catalog validation tests
```

## Quick Start

Install dependencies:

```powershell
python -m pip install -r projects/streaming-api-lakehouse/requirements.txt
```

Run tests:

```powershell
python -m pytest projects/streaming-api-lakehouse/tests
```

Start the Wikimedia live producer:

```powershell
python -m streaming_api_lakehouse.producers.wikimedia_eventstreams `
  --catalog projects/streaming-api-lakehouse/configs/streaming_apis.json `
  --bootstrap-servers localhost:9092 `
  --topic api.raw.events
```

Run the Bronze Spark stream:

```powershell
spark-submit projects/streaming-api-lakehouse/src/streaming_api_lakehouse/spark/bronze_stream.py `
  --bootstrap-servers localhost:9092 `
  --topic api.raw.events `
  --checkpoint-path /tmp/streaming-api-lakehouse/checkpoints/bronze `
  --output-path /tmp/streaming-api-lakehouse/delta/bronze_api_events
```

## Data Model

Bronze keeps the raw API event envelope:

- `source_api`
- `source_domain`
- `event_type`
- `event_time`
- `ingestion_time`
- `payload`
- `metadata`

Silver normalizes source-specific fields. The included Silver job targets Wikimedia recent-change events.

Gold aggregates near-real-time metrics such as events per minute by wiki, change type, and bot flag.

## Notes

Some APIs in the PDF are free but require API keys or fair-use limits. Keep keys in environment variables or your orchestration platform secrets, not in source control.
