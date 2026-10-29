# Compatibility and troubleshooting

| Symptom | What to check |
| --- | --- |
| samples.nyctaxi.trips unavailable | Check Catalog Explorer for the Databricks samples catalog and attach compute that can query it. Ask your workspace administrator about asset access. Do not substitute synthetic or external datasets. |
| Python module pyspark missing locally | These notebooks are intended for Databricks. Local static validation does not require PySpark and does not execute the notebooks. |
| Permission denied creating schema | Set target_catalog to a writable catalog; do not select samples. Use the minimum required catalog/schema grants. |
| CREATE VOLUME denied | Obtain permission to create/use a course volume or run file/streaming labs in a workspace where this is permitted. Do not redirect to arbitrary DBFS root/mounts. |
| Widget changes have no effect | Apply the widget change and rerun from the asset-loading cell. Each notebook is self-contained. |
| Historical dataset filtered by today gives no rows | Notebook 41 defaults the run date from the actual asset. A user-supplied date may legitimately be outside its range. |
| MERGE reports multiple source matches | Select one source row per stable business key and event sequence before merging. Matching conditions and duplicate behavior vary by runtime. |
| JSON round-trip assertion fails | Check timestamp format/timezone, decimal precision and schema changes. Keep the original payload for diagnosis. |
| Streaming sink gains duplicate rows after a fresh start | A fresh checkpoint cannot identify already processed data in an old sink. Use a stable checkpoint/sink identity, or reset only explicitly disposable course assets. |
| Stream fails after source UPDATE/DELETE | The basic Delta stream examples use append-only tables. Use CDF and explicit change-application logic for mutable sources. |
| Stream output has fewer historical rows than source | Inspect event-time watermarks, micro-batch ordering and late-row counts. Watermarks are not processing-time freshness checks. |
| RDD attribute/access error | RDD/mapPartitions labs require dedicated classic compute. Keep RUN_CLASSIC_RDD/RUN_DB2 disabled on serverless or standard compute. |
| Python UDF unsupported | Check environment/runtime support and dependency restrictions. Prefer the native expression from the same notebook. |
| JDBC driver/JVM unavailable | JDBC integration requires compatible classic/managed compute, a valid driver on workers and approved network access. Do not install a separate local PySpark. |
| JayDeBeApi cannot load the DB2 jar | Ensure the same compatible jar and Java access exist on every executor, not only the driver. Follow approved library distribution for your compute. |
| DB2 commit outcome unknown | Stop and reconcile target keys/payloads before retrying. A lost commit response does not prove the database rolled back. |
| Spark ML unavailable | Choose a recent environment with supported Spark ML APIs; do not replace the required Databricks asset with a local dataset. |

## Reruns and output boundaries

Every write table is prefixed with its lab number inside the per-user course schema. Table labs reset their own targets where the scenario requires a clean start. File/streaming labs use a unique run directory and matching unique stream tables; checkpoints remain stable inside restart tests. Optional JDBC/DB2 targets must be deliberately created dedicated database lab tables. Never redirect the examples to an existing operational table.

## Data and execution semantics

The taxi content hash is useful for exact-row replay, but not a substitute for a true stable source trip ID. Do not use it as a customer/account identity. Source times may need a documented timezone interpretation. A bounded output is not a bounded source scan. Asset-backed tests should use invariants and reconciliation instead of fixed counts.

## What the course intentionally does not provision

It does not provision cloud storage accounts, Event Hubs/Kafka brokers, service principals, network grants, secret scopes, production permissions or a Lakeflow pipeline deployment. Auto Loader and streaming examples use asset-derived Delta/files in your course namespace. JDBC/DB2 integrations are explicit optional patterns. Lakeflow AUTO CDC is discussed as a production option; the SCD2 notebook implements batch event-history reconstruction.
