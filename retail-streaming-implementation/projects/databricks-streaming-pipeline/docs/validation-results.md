# Validation evidence

Validated locally on 2026-09-06 with Python 3.12 and Apache Spark 4.0.1.

- `python -m pytest tests -q --disable-warnings --maxfail=1`: **10 passed in 15.72 seconds**.
- `python -m ruff check .`: **All checks passed**.
- `python -m compileall -q src scripts tests`: passed.
- Bundle and pipeline/job YAML: parsed successfully with PyYAML (syntax check only).

Test coverage includes nested JSON and decimal coercion, malformed/null/invalid envelopes,
email/country/quantity validation, schema v2/v3, CDC before/after source projections,
array row cardinality, exact Gold arithmetic, and an actual file-based Structured
Streaming query across duplicates, watermark advancement, late arrivals, and repeated
checkpointed restarts.

An initial run hit an environment Python-worker daemon startup failure. Test inputs
were changed to JVM-native literal frames; production transforms were unchanged.

Not executed: Databricks bundle validate/deploy, Auto Loader, AUTO CDC engine,
managed expectations/materialized views, Unity Catalog grants, cloud-scale capacity,
or Databricks fixture acceptance. These require the intended workspace and identity.
`scripts/acceptance.py` and README release gates describe those checks explicitly.

GitHub API access check reports `pull=true`, `push=false`, `admin=false` for the
connected account on VreddyChittepu/modern-data-engineering. No remote changes are
claimed. Implementation is prepared as a local Git commit and downloadable patch.
