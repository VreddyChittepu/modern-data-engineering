# Operational gates and recovery

## Release

Use a reviewed commit and passing local CI. Deploy DEV, generate fresh fixture data, inspect quarantine. TEST must use an isolated catalog and a single fixture batch; run acceptance, restart unchanged source, run acceptance again. Then run representative volume/skew/outage tests with agreed freshness and cost budgets. Capture Databricks update IDs and results with the release. Configure GitHub protected environment reviewers and environment-scoped OAuth secrets before enabling production deployment.

## Rollback

Redeploy the previous reviewed commit without deleting the pipeline, checkpoints or landing files. If state/table schemas changed incompatibly, stop the update, restore the compatible version and plan an isolated rebuild/cutover. Code rollback does not roll back already-applied data mutations. Publish compensating source CDC with higher sequences for incorrect business updates, or use a reconciled backfill into new targets. Preserve raw, quarantine and event logs throughout.

## Source and retention

One NDJSON event per line. Files immutable after publication. Each event ID identifies exactly one payload and original event timestamp. Each source entity/key has a monotonic sequence, consistent business timestamp and a full after-image. Delete before-images retain keys. Raw retention must exceed replay requirements; Delta CDF retention must exceed consumer outages. Seven-day tombstones are an explicit example SLA, not an unlimited late-update guarantee.

## Metrics

Business audit tables measure records, rejection, duplicate identity and arrival delay. Pipeline runtime logs measure processing progress, duration, throughput and state/watermark drops. Alert on the actual metric available in the deployed runtime; never relabel arrival delay as processing latency. Protect raw event logs and quarantine because they contain customer PII. Grant monitoring readers only the required schema/view access.

## Identity conflicts

AUTO CDC converges deterministically by sequence, timestamp, event ID and payload hash. dq.cdc_conflicts detects disagreement at a source sequence; quarantine invalid structure before CDC, but do not claim semantic conflicts were withheld from AUTO CDC. A conflict requires investigation and a higher-sequence source correction. For a strict fail-closed source, add a durable upstream sequencer/contract validator before this pipeline.
