# Diabetes prediction learning project — Databricks + MLflow

[Open the step-by-step notebook](diabetes_prediction_databricks.ipynb), or import the [Databricks Python source export](diabetes_prediction_databricks.py).

The project classifies a genuine historical **Yes/No diabetes label**. It is an educational model, not a personal diagnosis, clinical screening service or a forecast of future disease. Dataset performance does not establish validity for the general population.

## What is included

- Databricks sample-asset preflight and schema/label checks.
- Missing-measurement handling and an explicit seven-feature contract.
- A prior baseline, logistic regression and random forest.
- Training-only cross-validation for model selection; separate validation-only threshold selection.
- Locked official test evaluation: ROC-AUC, average precision, precision, recall, specificity, F1, confusion matrix and Brier score.
- ROC, precision-recall, confusion-matrix and calibration plots generated from real runtime results.
- MLflow lineage, exact dependency versions, metrics, figure, signature, input example and reload checks.
- A packaged model that preserves the selected decision threshold.
- Optional Unity Catalog registration and course-only Delta prediction output, both off by default.

## Required source assets — availability still needs checking

The source contract is **MASS `Pima.tr.csv` and `Pima.te.csv` in a Databricks-provided sample asset directory**. The default candidate location is:

```text
/databricks-datasets/Rdatasets/data-001/csv/MASS/Pima.tr.csv
/databricks-datasets/Rdatasets/data-001/csv/MASS/Pima.te.csv
```

The individual paths have **not been verified in your workspace**. Databricks documents its sample-asset collections and warns that availability/location can change. The preflight must list the directory and find both exact files before training starts. If your workspace exposes the same files under the official `/Volumes/samples/databricks/datasets/` collection, browse that volume and set `asset_root` to the directory you actually observed; the notebook does not guess its internal layout.

If neither official sample location contains these files, the notebook stops with a missing-prerequisite error. Under your Databricks-assets-only requirement it cannot continue with a web download, uploaded replacement, Kaggle, synthetic patients, taxi records or `sklearn.load_diabetes`. The sklearn dataset is also a different regression task. A GitHub connection cannot inspect your Databricks filesystem.

No data is bundled in this repository or fetched from the web by the notebook. CSV row-name/index columns are ignored as features. The provenance log identifies parsed feature/label content, not patient identities or the original file bytes.

## Run in Databricks

1. Add this repository as a Git folder or import the notebook/source export into Workspace.
2. Attach Python compute that can read the required Databricks sample files. Legacy DBFS asset access may depend on workspace/compute configuration.
3. In the notebook Environment panel configure compatible versions: `numpy>=1.24,<3`, `scipy>=1.11,<2`, `scikit-learn>=1.4,<2`, `matplotlib>=3.8,<4`, `mlflow>=2.20,<4`. Apply/restart the environment. Do not install another PySpark over the provided package.
4. Run top to bottom. Resolve the source preflight before attempting training. No pandas code or conversion is needed.
5. Open the printed MLflow run and inspect its metrics and artifact round-trip check. Metrics are computed at runtime; the repository contains no invented scores or prediction examples.
6. Enable registration or output-table writing only after setting a writable `target_catalog`. The outputs use a per-user `diabetes_learning_<hash>` schema and never write to sample assets. No serving endpoint is created.

## How the split prevents leakage

The source’s 200-row training file is split into **160 fit rows + 40 validation rows**. Five-fold CV on the fit rows selects among ten configurations. The 40 validation rows choose an educational F1 threshold. The model is deliberately **not refitted afterward**, so the selected cutoff is applied to the exact fitted estimator evaluated on validation. The official **332-row test file** is scored only after both choices are locked.

Imputation/scaling live inside the training pipeline. The target, CSV row index and any derived label proxy are excluded from the feature list. Exact feature overlap between development/test and repeated development feature rows stop training for review. These checks cannot establish person-level independence because the export does not expose a verified subject identifier.

Repeatedly changing the model after reading official-test metrics makes that test exploratory. Use a new independently collected holdout for a new claim of untouched evaluation.

## Feature contract

| Field | Meaning |
| --- | --- |
| `npreg` | Pregnancy count; zero is allowed |
| `glu` | Glucose measurement during a tolerance test |
| `bp` | Diastolic blood pressure |
| `skin` | Skinfold thickness |
| `bmi` | Body mass index |
| `ped` | Pedigree-function value |
| `age` | Age |
| `type` | Source target: `No → 0`, `Yes → 1`; excluded from inputs |

The packaged tensor requires exactly these seven ordered inputs. It returns `[model_probability_of_Yes, predicted_historical_label]`. The probability is not a clinically certified risk estimate. Do not input personal health information into this learning notebook.

## Validation status

Run `python tests/validate_project.py` locally. It checks notebook/source syntax and equality, source-boundary guards, no substitute dataset imports/downloads, intended fitting inputs and disabled optional writes. It also executes source-path validation tests using strings only. It does **not** train on fabricated patient records.

Source-file existence, parsing, training, MLflow logging, model reload, charts, registration and Delta output have **not been runtime-tested in your Databricks workspace**. The notebook includes executable checks for those stages. See [VALIDATION.md](docs/VALIDATION.md) and the generated report.

## Further learning

Inspect how model selection changes with seeds, how validation size affects threshold stability, and how calibration should be fitted without using test labels. For any real medical purpose, representative external/prospective evaluation, clinical oversight and appropriate governance are separate requirements. Glucose measurements are related to label ascertainment, so dataset discrimination is not evidence of early prediction utility.

References: [MASS dataset documentation](https://stat.ethz.ch/R-manual/R-devel/library/MASS/html/Pima.tr.html), [Databricks sample assets](https://docs.databricks.com/aws/en/discover/databricks-datasets), [GridSearchCV](https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.GridSearchCV.html), and [MLflow Python models](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.pyfunc.html).
