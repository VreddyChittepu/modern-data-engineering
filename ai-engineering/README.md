# AI Engineering Hands-on in Databricks

A step-by-step course with **30 independent Python notebooks**, from numerical foundations and Spark to MLflow, RAG, agents and deployment. Every notebook has goals, prerequisites, explained code cells, expected checks, a practice exercise and a solution guide. This covers the major practical areas of AI engineering; it is not an exhaustive treatment of every algorithm or vendor feature.

## Start here

1. In Databricks Workspace, create a **Git folder** using this repository URL. If Git folders are unavailable, download an `.ipynb` from GitHub and import it into Workspace.
2. Open [00 — Setup](notebooks/00_setup_and_learning_path.ipynb), attach Python compute and configure the notebook Environment packages.
3. Run each notebook top to bottom. Start with 00–09, continue through 10–18, then 19–24, and finish with 25–29.
4. Each notebook recreates its own small data and required Python objects. No `%run`, implicit notebook state, pandas conversion, API key, private dataset or external dataset download is needed for core labs.

## Execution modes

- **portable:** CPU Python with NumPy, SciPy and sklearn. Runs in Databricks after setup; local execution checks cover these cells. A few contain optional live endpoint blocks disabled by default.
- **databricks:** requires a real Databricks Spark session, appropriate Unity Catalog privileges and/or Managed MLflow. These notebooks have been checked statically but have not been executed against your workspace.
- **optional:** dry run by default. Endpoint creation, managed search, and LoRA training require explicitly setting the switches and configuration inside the notebook. A skipped service call is never represented as a successful deployment or LLM response.

Free Edition uses serverless compute and has quotas, permission and network restrictions. Availability varies by workspace and can change. No course promise depends on a particular free endpoint or GPU. The local retrieval and NumPy neural-network labs work without model downloads. Spark ML requires a recent supported environment; use the sklearn equivalents if unavailable.

## Notebook map

| Lab | Concepts | Execution |
| --- | --- | --- |
| 00 | [Setup and learning path](notebooks/00_setup_and_learning_path.ipynb) | portable |
| 01 | [Python and NumPy for AI](notebooks/01_python_numpy.ipynb) | portable |
| 02 | [Math, probability, and gradient descent](notebooks/02_math_gradient_descent.ipynb) | portable |
| 03 | [Spark, Delta Lake, and data quality](notebooks/03_spark_delta_data_quality.ipynb) | databricks |
| 04 | [EDA, preprocessing, and data leakage](notebooks/04_features_and_leakage.ipynb) | portable |
| 05 | [Regression: baseline, linear model, and regularization](notebooks/05_regression.ipynb) | portable |
| 06 | [Classification, ROC-AUC, PR-AUC, and thresholds](notebooks/06_classification_metrics.ipynb) | portable |
| 07 | [Decision trees, random forests, and boosting](notebooks/07_trees_and_ensembles.ipynb) | portable |
| 08 | [Cross-validation and hyperparameter tuning](notebooks/08_cross_validation_tuning.ipynb) | portable |
| 09 | [Clustering, PCA, and anomaly detection](notebooks/09_unsupervised_learning.ipynb) | portable |
| 10 | [Time series forecasting and backtesting](notebooks/10_time_series_forecasting.ipynb) | portable |
| 11 | [Recommendation and ranking](notebooks/11_recommender_systems.ipynb) | portable |
| 12 | [Distributed ML with Spark DataFrames](notebooks/12_spark_ml_pipeline.ipynb) | databricks |
| 13 | [MLflow experiment tracking and model packaging](notebooks/13_mlflow_experiment_tracking.ipynb) | databricks |
| 14 | [Unity Catalog registry and batch inference](notebooks/14_registry_and_batch_inference.ipynb) | databricks |
| 15 | [Deploy and query a model serving endpoint](notebooks/15_model_serving.ipynb) | optional |
| 16 | [Neural networks and backpropagation from scratch](notebooks/16_neural_networks.ipynb) | portable |
| 17 | [NLP, tokenization, and text classification](notebooks/17_nlp_text_classification.ipynb) | portable |
| 18 | [Transformers: tokenization, embeddings, and causal attention](notebooks/18_transformers_attention.ipynb) | portable |
| 19 | [Embeddings and similarity search](notebooks/19_embeddings_similarity.ipynb) | portable |
| 20 | [Prompt engineering and structured outputs](notebooks/20_prompting_structured_outputs.ipynb) | portable |
| 21 | [RAG: chunking, retrieval, citations, and abstention](notebooks/21_rag_pipeline.ipynb) | portable |
| 22 | [Managed vector search over Delta data](notebooks/22_managed_vector_search.ipynb) | optional |
| 23 | [Agents, tool calling, and bounded execution](notebooks/23_agents_and_tool_calling.ipynb) | portable |
| 24 | [GenAI evaluation and retrieval regression tests](notebooks/24_genai_evaluation.ipynb) | portable |
| 25 | [Fine-tuning and LoRA](notebooks/25_fine_tuning_lora.ipynb) | optional |
| 26 | [Monitoring, fairness, and explainability](notebooks/26_monitoring_fairness_explainability.ipynb) | portable |
| 27 | [Capstone: churn model from Delta to MLflow predictions](notebooks/27_end_to_end_capstone.ipynb) | databricks |
| 28 | [Vision and audio foundations](notebooks/28_multimodal_foundations.ipynb) | portable |
| 29 | [Security, reliability, caching, and cost controls](notebooks/29_security_reliability_cost.ipynb) | portable |

## Dependencies and reproducibility

Core: `numpy>=1.24,<3`, `scipy>=1.11,<2`, `scikit-learn>=1.4,<2`. These are supported ranges, not a fully tested version matrix. Local numerical checks were run with NumPy 2.3.5, SciPy 1.17.0 and scikit-learn 1.8.0. Use the packages bundled with your environment when compatible and record the actual versions with experiment results.

MLflow labs: `mlflow>=2.20,<4`. Live calls: `databricks-sdk>=0.40,<1`, `openai>=1.60,<3`. Managed search: `databricks-vectorsearch>=0.50,<1`. LoRA: `torch>=2.2,<3`, `transformers>=4.45,<5`, `peft>=0.13,<1`. Optional integrations have not been runtime-tested here. Do not install a separate PySpark package inside Databricks.

Set package versions in the notebook Environment panel, apply, then restart and rerun. On classic compute notebook-scoped `%pip` is an alternative. Never put tokens in notebook code, outputs or Git. Use Databricks native authentication or organization-approved secret handling.

## Data, writes and cleanup

All examples use synthetic, hand-authored or sklearn-bundled data. Spark labs create a per-user `ai_lab_<hash>` schema in the current catalog; change to a writable catalog if necessary. Only explicitly named course tables are overwritten. Re-running table labs replaces those course table contents; MLflow re-runs add experiment runs. Registration adds a version only when enabled.

Live GenAI sends course text to the endpoint you choose. Endpoint creation can consume quota or incur cost in paid workspaces; it is off by default. Managed search uses an existing endpoint and an explicitly named course source/index. Review those names before opting in. Clean up course serving endpoints/indexes in the UI after the labs; preserve shared resources. Tables/experiments can be retained for review. Never run a broad DROP SCHEMA CASCADE to clean a shared namespace.

## Suggested capstone outcomes

- ML track: a Delta feature snapshot, split protocol, MLflow run, signed model, immutable model reference, prediction table and monitoring design.
- GenAI track: labeled retrieval cases, Recall@k/MRR, optional real model responses, citation/abstention review, bounded read-only agent, access-filtering and cost tests.
- Explain why a model improves on a baseline, what the evaluation cannot prove, and how to reproduce the result.

## Verify locally

From this folder run `python tests/validate_notebooks.py`. It validates notebook structure and Python syntax for all notebooks and executes every portable notebook plus the disabled optional paths in isolated namespaces. It intentionally does not emulate Spark, Managed MLflow, cloud model endpoints, LoRA training or remote services. See [validation](docs/VALIDATION.md).

## Reference and troubleshooting

See [official references](docs/REFERENCES.md) and [Databricks troubleshooting](docs/TROUBLESHOOTING.md). Read the current vendor documentation if an SDK or workspace feature differs from this course.
