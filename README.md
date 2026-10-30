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
