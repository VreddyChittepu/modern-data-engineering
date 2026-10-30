# Validation status

Local validation on 30 October 2026 checks notebook/source Python syntax and equivalence, empty committed outputs, permitted Databricks source boundaries, no external/substitute dataset imports, model fit-input references, text-overlap filtering and disabled optional AI/table/text-logging switches. It executes nine path-guard cases and ten rating-policy cases using configuration metadata only. No synthetic review dataset is created.

The [machine-readable report](validation_results.json) records the checks. Reproduce with `python tests/validate_project.py`.

## Not runtime verified here

- Databricks asset availability, schema and actual retained label balance.
- Spark ingestion, profile counts and bounded collection.
- TF-IDF/classifier cross-validation, threshold selection, held-out metrics and charts.
- MLflow signature/logging/artifact reload and prediction consistency.
- Optional SQL AI sentiment inference or Delta output.

A GitHub connection cannot execute those workspace checks. The notebook includes assertions and visible failures at each stage; no metric, model URI or service response is fabricated. The default sample paths are candidates to validate on your compute, not a guarantee of workspace availability.

## Runtime acceptance sequence

1. Preflight must read the genuine Databricks review assets with review/rating fields; stop if unavailable instead of substituting data.
2. Check all quality/exclusion counts and class sizes. Do not interpret a failed source contract as a completed training run.
3. Assert no exact normalized-text overlap between fit/validation/holdout and confirm the vocabulary/IDF fit only inside training folds.
4. Record held-out scores against the prior baseline without forcing a minimum score. Confirm what the rating proxy does and does not label.
5. Inspect the actual MLflow run, signed threshold-aware model and reload equality check.
6. Only if explicitly enabled, inspect real AI responses and comparable coverage; retain neutral/mixed/null as outside the binary comparison. Confirm output tables stay in the course namespace.
