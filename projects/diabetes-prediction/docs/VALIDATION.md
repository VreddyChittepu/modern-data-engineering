# Validation status

Local checks performed on 30 October 2026 cover notebook and source-export syntax/equivalence, empty committed outputs, source path boundaries, absence of substitute dataset imports/downloads, fit-input references and disabled optional registry/table writes. Nine actual source-path guard cases execute using strings only; no patient data is synthesized for tests.

The project has no live Databricks workspace connection. The exact Pima assets, CSV parsing, numerical training/evaluation, plots, MLflow tracking/signature/model reload and optional registration/table writes are **not runtime verified**. The documentation confirms the historical dataset definition and Databricks asset collection, not the availability of the individual candidate files on your compute. No accuracy, clinical validity or deployment success is claimed.

The [machine-readable local report](validation_results.json) records the local checks. Run `python tests/validate_project.py` to reproduce them without installing Spark or loading a clinical dataset.

## Runtime acceptance checks

1. Preflight must locate the exact Databricks sample files with the expected schema, genuine Yes/No labels and documented split sizes. Stop if they are unavailable; do not substitute an external or invented dataset.
2. The notebook must pass source value/overlap checks and make a deterministic 160/40 development split while keeping the official 332-row test file for evaluation.
3. All ten CV candidate configurations must fit using training rows only; the threshold must be chosen from validation only.
4. Model/baseline metrics, confusion matrix and charts must be computed from actual held-out labels, without any guaranteed score threshold.
5. MLflow must store the parsed-data lineage, feature order, dependency versions, selected threshold and exact estimator. Reloaded model outputs must match the evaluated notebook predictions.
6. Optional writes must remain confined to the explicitly chosen course schema. No clinical endpoint is created by this project.

If a runtime acceptance check fails, retain the error and environment details. Do not replace a failed result with a mock success or report a locally compiled notebook as a completed training run.
