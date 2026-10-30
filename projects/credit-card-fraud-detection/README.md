# Credit-card fraud detection in Databricks

[Open the step-by-step notebook](credit_card_fraud_databricks.ipynb) or import the equivalent [Databricks Python source export](credit_card_fraud_databricks.py).

This project learns a fraud-review classifier from genuine labeled transactions in the Databricks sample asset, with chronological evaluation, class weighting, review-budget threshold selection and MLflow tracking. No pandas conversion, external dataset download, generated transaction or synthetic minority dataset is used.

## Required Databricks asset

Default candidate source:

```text
/databricks-datasets/credit-card-fraud/data
```

The Parquet asset has this documented contract:

| Field | Role |
| --- | --- |
| `pcaVector` | 28 supplied anonymized PCA coordinates |
| `amountRange` | Categorical amount bucket, codes 0–7; not exact money |
| `time` | Relative source seconds, used for chronological splitting |
| `label` | Genuine historical target: 0 non-fraud, 1 fraud |

The model input is the 28 coordinates followed by the amount bucket. Time, target and content fingerprint are excluded from model features. This asset differs from common CSV examples containing `V1`–`V28`, `Amount` and `Class`; the notebook does not silently convert another dataset into this contract.

The path/schema are grounded in Databricks-hosted examples, but availability in **your workspace is unverified**. The runtime preflight must read the actual asset. If an official `/Volumes/samples/databricks/datasets/` location exposes the same asset, set `source_path` to its observed location; no internal volume path is guessed. Other locations, external CSVs, uploads and generated-data fallback are rejected.

The source remains read-only. No source transactions or model metrics are bundled in this GitHub project. Retain the dataset attribution and inspect the asset’s accompanying license/description before using derived artifacts beyond the learning exercise.

## Learning workflow

1. Check source schema and values; use native `vector_to_array` for supplied vectors.
2. Reconcile source/invalid/valid counts and inspect natural label imbalance.
3. Detect conflicting labels and deduplicate exact time/bucket/vector replays using a content key.
4. Take the earliest bounded chronological slice, default **100,000** records; retain observed class proportions.
5. Split on distinct source-time boundaries into earlier fit, later validation, and latest test intervals. Equal-time transactions stay together.
6. Use three expanding forward-time CV folds within fit data. Their actual time durations and fraud counts are reported.
7. Fit scaling/one-hot encoding inside each fold and compare four weighted logistic-regression/random-forest configurations by average precision.
8. Choose a validation threshold maximizing fraud recall under an illustrative **1% validation review-volume budget**, with explicit ties and zero-alert handling.
9. Evaluate on the locked later-period test interval against an always-legitimate baseline: AP, ROC-AUC, precision/recall, false alarms, missed frauds, alert rate and Brier score.
10. Report top-budget ranking metrics separately from threshold-policy results, plot PR/ROC/confusion, and inspect anonymized feature importance/error hashes.
11. Package preprocessing and the exact chosen cutoff; log provenance, folds, class counts, dependencies, metrics, signature and model in MLflow.
12. Reload and verify scores/flags against the notebook; optionally write a course-only Delta prediction table.

## Run

1. Add this repository as a Databricks Git folder or import the notebook/source export.
2. Attach recent Python compute supporting the sample asset and Spark vector APIs. Add compatible `numpy>=1.24,<3`, `scipy>=1.11,<2`, `scikit-learn>=1.4,<2`, `matplotlib>=3.8,<4` and `mlflow>=2.20,<4` in the notebook Environment panel. Apply/restart; keep the PySpark supplied by Databricks.
3. Run top to bottom. Asset preflight must pass before training. If an interval/CV fold lacks enough real frauds, increase the genuine source slice or investigate the time coverage. No shuffled/synthetic fallback is applied.
4. Inspect actual metrics and the printed MLflow run. No minimum accuracy/AP is guaranteed. A high always-legitimate accuracy is not successful fraud detection.
5. `SAVE_PREDICTIONS=False` and `LOG_INPUT_EXAMPLE=False` by default. Optional table output uses a writable catalog and per-user `fraud_learning_<hash>` schema. No serving endpoint, payment action, outbound alert or banking connection is created.

## Evaluation and interpretation

The source-time split uses approximately 60%/20%/20% of distinct timestamps; row fractions may differ. The same time value cannot cross a period or CV fold. No downsampling, oversampling or synthetic generation changes validation/test prevalence. Class weights are learned only during training. Average precision is not identical to a trapezoidal implementation of PR-AUC and depends on prevalence.

The fitted estimator is deliberately not refitted after threshold selection. The test score distribution may produce a different alert rate from validation; the notebook reports that change without retuning from test labels. If tied scores prevent any nonzero-alert threshold satisfying the validation budget, the zero-alert result is explicit. Top-k ranking breaks ties by content key and is a different policy from flagging all scores at a threshold.

Class-weighted model scores are not automatically calibrated fraud probabilities. The supplied PCA transformation’s upstream fitting split is unknown, so downstream chronological splitting cannot establish leakage-free upstream preprocessing. The content hash is not a verified bank transaction/cardholder ID. Authoritative identities, label delays, real operational drift and prospective validation are outside this sample’s evidence.

`amountRange` does not reveal exact transaction amounts. The project does not estimate dollars saved/lost or invent per-transaction costs. A review flag is not proof of fraud or authorization to decline a payment. Production deployment requires an agreed review/decision policy, calibration, delayed-label handling, monitoring and representative prospective evaluation.

## Validation status

Run `python tests/validate_project.py` for syntax, notebook/source equivalence, dataset-boundary guards, alert-budget policy guards, fitting-input references, chronological split checks and disabled optional output. No fabricated transaction dataset is used for local checks.

Databricks source access, Spark quality checks, numeric training/evaluation, charts, MLflow artifact reload and optional Delta output have **not been runtime-executed in your workspace**. See [validation details](docs/VALIDATION.md) and the generated report.

## References and attribution

- [Databricks-hosted original vector/amount-range fraud example](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/1212468499849361/649180143457189/7151759854724023/latest.html)
- [Databricks sample dataset availability](https://docs.databricks.com/aws/en/discover/databricks-datasets)
- [Spark vector_to_array](https://spark.apache.org/docs/3.5.6/api/python/reference/api/pyspark.ml.functions.vector_to_array.html)
- [Forward-time split API](https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.TimeSeriesSplit.html)
- [Average precision](https://scikit-learn.org/stable/modules/generated/sklearn.metrics.average_precision_score.html)
- [MLflow Python models](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.pyfunc.html)

Research attribution carried with the source: Andrea Dal Pozzolo, Olivier Caelen, Reid A. Johnson and Gianluca Bontempi, *Calibrating Probability with Undersampling for Unbalanced Classification*, CIDM (2015). Refer to the asset’s bundled license for its data-use terms.
