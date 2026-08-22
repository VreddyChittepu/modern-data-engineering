# openFDA Safety Data Model

## Bronze

Bronze stores raw API responses with minimal interpretation:

- `endpoint_name`
- `source_url`
- `ingestion_time`
- `raw_record`

## Silver

Silver extracts common adverse-event fields across drug, device and food event records:

- `event_id`
- `endpoint_name`
- `event_date`
- `is_serious`
- `reaction_terms`
- `product_names`
- `raw_record`

Field availability varies by openFDA endpoint, so Silver keeps the raw JSON for replay and debugging.

## Gold

Gold tables answer safety analytics questions:

- adverse events by day and endpoint
- serious adverse events by day
- top reaction terms
- product-level event counts

## Incremental Strategy

Use each endpoint's date field for incremental pulls:

- Drug adverse events: `receivedate`
- Device adverse events: `date_received`
- Food adverse events: `date_started`

Persist the high watermark after every successful load.
