# Modern Data Engineering

Repository for modern data engineering notes, examples, patterns, and projects.

## Structure

- `pyspark/`
- `spark/`
- `databricks/`
- `delta-lake/`
- `unity-catalog/`
- `azure-data-factory/`
- `sql/`
- `streaming/`
- `data-quality/`
- `performance-tuning/`
- `design-patterns/`
- `projects/`
- `ai-engineering/`

## Projects

- `projects/customer-churn-pipeline/`
- `projects/openfda-safety-analytics/`
- `projects/streaming-api-lakehouse/`

## AI engineering hands-on

[Start the Databricks AI engineering course](ai-engineering/README.md): 30 step-by-step notebooks covering Python/NumPy, math, Spark/Delta, feature engineering, ML algorithms and evaluation, MLflow, registry/batch inference, serving, neural networks, NLP/transformers, embeddings, RAG, managed search, agents, GenAI evaluation, LoRA, monitoring, multimodal foundations and an end-to-end capstone.

The course includes dependency setup, exercises, solution guidance, official references and a validation report. Core labs use small local datasets; Databricks and optional service prerequisites are explicitly marked.

## Databricks PySpark scenarios

[Open the PySpark hands-on course](pyspark/databricks-scenarios/README.md): 50 step-by-step notebooks using only Databricks-provided `samples.nyctaxi.trips` record data. Covers DataFrames, joins, windows, nested structures, SQL, Delta, SCD/CDC, incremental ETL, Auto Loader, streaming, performance, governance, jobs, testing, reconciliation and optional DB2/JDBC integration. Includes asset-backed assertions, pytest examples, setup guidance and a static validation report.

## Diabetes prediction learning project

[Open the Databricks + MLflow diabetes notebook](projects/diabetes-prediction/README.md): step-by-step asset preflight, historical binary-label classification, model comparison, leakage-aware evaluation, threshold-aware packaging and MLflow tracking. Requires genuine Pima training/test files in a Databricks-provided sample asset location; individual workspace asset availability remains unverified until its preflight passes. Educational use only; no clinical performance or diagnostic deployment is claimed.

## Sentiment analysis learning project

[Open the Databricks + MLflow sentiment notebook](projects/sentiment-analysis/README.md): real Databricks Amazon review assets, rating-derived binary sentiment proxies, duplicate-text leakage checks, TF-IDF, logistic regression/Naive Bayes comparison, evaluation charts, threshold-aware packaging and MLflow tracking. Includes an optional bounded SQL AI sentiment comparison, disabled by default. Asset availability and runtime training remain to be checked in your Databricks workspace.

## Credit-card fraud detection learning project

[Open the Databricks + MLflow fraud notebook](projects/credit-card-fraud-detection/README.md): genuine Databricks fraud labels with the PCA-vector/amount-range asset schema, chronological splits, training-only class weighting, AP-based model comparison, validation review-budget threshold selection, held-out error/ranking metrics and threshold-aware MLflow packaging. Dataset access and runtime training remain unverified in your workspace. Produces review flags only; no automatic payment decision or banking integration is implemented.
