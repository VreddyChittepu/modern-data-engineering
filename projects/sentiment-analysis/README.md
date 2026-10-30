# Sentiment analysis in Databricks

[Open the step-by-step notebook](sentiment_analysis_databricks.ipynb) or import the equivalent [Databricks Python source export](sentiment_analysis_databricks.py).

This project reads real Amazon reviews from Databricks sample assets, trains a sparse TF-IDF classifier, evaluates it without exact-text leakage and tracks/packages the model with MLflow. It uses Spark for ingestion and bounded NumPy/scikit-learn training, with no pandas conversion.

## Source and labels

The default candidate assets are:

```text
/databricks-datasets/amazon/data20K
/databricks-datasets/amazon/test4K
```

Both must be Parquet with `review` text and numeric `rating` columns. Databricks-hosted review examples support this data family, but the exact assets have **not been verified in your workspace**. The runtime preflight checks availability and schema. If the same assets exist under the official `/Volumes/samples/databricks/datasets/` collection, set `asset_root` to their observed parent directory. No internal volume layout is guessed.

No external dataset download, uploaded replacement, generated review or sklearn dataset is used. The notebook stops if the required assets cannot be accessed. No review data is copied into this repository.

Actual star ratings supply **weak sentiment proxies**:

| Rating | Binary proxy |
| --- | --- |
| 1 or 2 | Negative, label 0 |
| 3 | Excluded from the binary task |
| 4 or 5 | Positive, label 1 |

These are not human-annotated sentiment labels. Three stars do not automatically mean neutral. The two-class model does not claim to detect neutral or mixed sentiment; sarcasm and rating/text disagreement remain important errors.

## What you will learn

1. Validate a Databricks text asset and record quality counts.
2. Normalize case, HTML and whitespace while preserving negation words.
3. Deduplicate normalized text, exclude conflicting-label text, and remove development/test text overlap.
4. Split development data into fit and validation sets, keeping a separate asset-derived holdout.
5. Build unigram/bigram TF-IDF inside cross-validation, avoiding vocabulary/IDF leakage.
6. Compare a prior baseline, logistic regression and multinomial Naive Bayes.
7. Select a cutoff on validation macro-F1, then evaluate on the locked holdout.
8. Inspect class metrics, ROC/PR plots, confusion matrix, learned terms and error IDs.
9. Log lineage, exclusions, dependency versions, CV summaries, metrics, plots and a threshold-aware signed model in MLflow.
10. Reload the artifact and verify its predictions match the evaluated notebook.
11. Optionally compare up to ten genuine held-out reviews with `ai_analyze_sentiment` and save a course prediction table.

## Run

1. Add the repository as a Databricks Git folder or import the notebook/source export.
2. Attach Python compute able to read the sample assets. Configure compatible `numpy>=1.24,<3`, `scipy>=1.11,<2`, `scikit-learn>=1.4,<2`, `matplotlib>=3.8,<4` and `mlflow>=2.20,<4` in the notebook Environment panel. Apply/restart and run top to bottom. Use Databricks-provided PySpark.
3. Run source preflight first; resolve asset access instead of substituting data.
4. Default caps are 8,000 development and 4,000 candidate-test rows, with reviews up to 12,000 characters. Sorting/capping limits driver data, not source scan cost. Deduplication and quality checks reduce those counts; the notebook reports each exclusion.
5. Inspect the printed experiment/run and artifact round-trip assertion. There are no fabricated scores or committed notebook outputs.
6. Keep `RUN_AI_FUNCTION=False` unless you want live AI inference on supported compute/region. That function may consume quota/incur cost and returns positive/negative/neutral/mixed or null. The notebook reports comparable coverage rather than forcing every result into the binary rating task.
7. Keep `SAVE_PREDICTIONS=False` unless you want a per-user course Delta table. Set a writable `target_catalog`; the `samples` catalog is protected. No serving endpoint or external message is created.

## Evaluation boundaries

The development asset is sampled deterministically by source-text hash, cleaned and deduplicated before a stratified 80/20 fit/validation split. Three-fold CV on fit rows selects among five configurations. The validation split selects the threshold. The fitted estimator is deliberately not refitted afterward, so that exact probability/threshold pairing is preserved for holdout scoring.

Candidate test text present in development is removed before evaluation. The holdout is therefore a cleaned/capped/overlap-filtered subset, not a claim about every record in the original test directory. Exact text exclusion cannot rule out near-duplicate, shared-author, shared-product or temporal leakage. Use grouped/time-based evaluation when the appropriate metadata exists. Repeated test-driven tuning makes the test exploratory.

Scores measure agreement with the declared rating proxy. Establishing actual sentiment quality requires a representative human-reviewed dataset and an annotation rubric. Neither a strong proxy score nor the optional ten-review AI comparison supplies that evidence.

## Model contract and data handling

Input: a one-dimensional NumPy string tensor of nonblank reviews within the character cap. Output: `[positive_probability, predicted_rating_proxy_label]`, where 0 is negative and 1 is positive. Normalization, the fitted vocabulary/IDF/classifier and the chosen cutoff travel together.

`PRINT_REVIEW_TEXT` and `LOG_RAW_INPUT_EXAMPLE` are false by default. Error inspection and output tables use text hashes rather than raw review bodies. Learned vocabulary/features still contain training tokens; treat model artifacts according to your data policy. Source assets remain read-only.

## Validation

Run `python tests/validate_project.py` for notebook/source equivalence, Python syntax, source-boundary checks, rating-map checks, training-input references and disabled optional switches. It uses no invented review dataset. Databricks asset reads, model training, charts, MLflow, SQL AI and optional table output have **not been executed in your workspace**. See [validation details](docs/VALIDATION.md).

References: [Databricks sample assets](https://docs.databricks.com/aws/en/discover/databricks-datasets), [Databricks review-model demonstration](https://www.databricks.com/wp-content/uploads/2015/10/spark-summit-eu-joseph-demo_-_1.html), [scikit-learn text features](https://scikit-learn.org/stable/modules/feature_extraction.html#text-feature-extraction), [MLflow Python models](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.pyfunc.html), and [Databricks AI sentiment function](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_analyze_sentiment).
