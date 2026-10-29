# Validation and runtime verification

Final local static check on 29 October 2026: **50 notebooks, 172 Python code cells, zero failures**. This includes asset-only source checks and disabled optional integration switches. The separate five-test pytest suite compiles but was not executed against Spark here.

The local validation script checks all 50 notebooks for JSON/cell structure, Python syntax, manifest consistency, empty committed outputs, the required Databricks source asset, the absence of substitute dataset imports/generators, protected output namespaces and disabled optional integration/mutation switches.

It does **not execute Spark**, access `samples.nyctaxi.trips`, create Databricks tables/volumes, execute streams, train Spark ML, connect to JDBC/DB2, or test optional platform features. No Databricks workspace connection is available through the GitHub connection. Runtime correctness and compute compatibility must be checked in Databricks. See the generated [validation_results.json](validation_results.json) for the static results.

Every notebook contains executable runtime assertions over the actual source asset. These check schema contracts, valid ranges, row accounting, join cardinality, deduplication, round-trip equality, merge replay behavior, history invariants, source/sink reconciliation and output contracts as appropriate. Expected-output text deliberately avoids fabricated concrete records or universal performance claims.

## Suggested runtime verification order

1. Run 00, 01, 03, 07, 09, 12, 14 and 19 on your intended compute.
2. Configure a writable catalog and run 22, 23, 25, 26, 27 and 29. Check only the course targets named in their output.
3. With volume privileges, run 30, 31, 32 and 34. Inspect query exceptions and checkpoint-restart assertions.
4. Run 33/35 and inspect event-time progress rather than assuming every historical arrival order produces identical results.
5. Run 48 only on compute supporting Spark ML. Keep integrations and legacy blocks disabled unless their individual prerequisites are satisfied.
6. Enable JDBC/DB2 only for dedicated disposable database lab targets, approved credentials/networking and an agreed retry/reconciliation policy.

When recording runtime results, capture the notebook filename, code commit, Databricks environment/runtime, access mode, sample limit, source asset visibility, target namespace, query/run IDs and assertion outcomes. Do not attach secret values or database connection details.
