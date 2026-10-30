# Validation status

Local checks on 30 October 2026 validate all 16 Python code cells and the equivalent Databricks source export, source-boundary policy, no substitute dataset imports, training fit-input references, forward-time split checks, AP model selection, validation-only threshold selection and disabled optional output. Nine source-path cases and twelve alert-budget policy cases execute using metadata only. No financial transaction data is generated for these checks.

The [machine-readable report](validation_results.json) records results. Run `python tests/validate_project.py` to reproduce them; NumPy is required for policy validation but no Spark session or dataset is read.

## Not runtime verified here

- The individual Databricks source asset’s availability, vector encoding and row/label counts.
- Spark conversions, invalid-value accounting, deduplication and the retained chronological slice.
- Class sufficiency in each period/CV fold and full training/evaluation.
- Review-threshold selection against actual scores, plots, feature/error inspection.
- MLflow logging/signature/artifact reload and optional Delta prediction output.

GitHub access does not provide a Databricks workspace execution connection. No trained model, score, deployment or financial benefit is claimed by a static validation pass.

## Runtime acceptance checks

1. Preflight reads genuine Databricks fraud Parquet with the documented vector/amount-range/label/time schema; stop on unavailable or incompatible data.
2. Quality/replay counts reconcile. Chronological periods and folds keep identical source times together and contain enough real minority examples.
3. Training-only class weights/preprocessing and AP selection fit on earlier rows. No evaluation resampling is used.
4. Validation threshold follows the review-volume budget, with explicit zero-alert/tied-score behavior. Test results do not tune that cutoff.
5. Test confusion totals reconcile to held-out rows and metrics compare against the all-legitimate baseline. Top-k ranking and threshold-policy outcomes remain separate.
6. MLflow stores source/split/fold provenance, class counts, dependencies and the exact threshold-aware model. Reloaded outputs must match the evaluated notebook.
7. Optional table output stays in the named course namespace. No bank/payment/notification action occurs.
