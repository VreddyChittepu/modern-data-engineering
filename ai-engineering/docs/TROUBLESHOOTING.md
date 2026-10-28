# Databricks troubleshooting

| Symptom | Action |
| --- | --- |
| ModuleNotFoundError | Add only the needed packages in the notebook Environment panel, apply changes, restart and run from the top. |
| NameError: spark | Attach Databricks Python compute; Spark labs cannot run in a plain local Python interpreter. |
| CREATE SCHEMA permission denied | Select a writable Unity Catalog catalog first or ask the catalog owner for appropriate grants. Preserve the per-user course namespace. |
| Missing MLflow experiment permissions | Use a writable personal Workspace experiment path; verify your workspace identity and experiment permissions. |
| Spark ML unavailable | Use a recent supported compute/environment version. Use the sklearn version of the lab while support is resolved. Do not install a different PySpark into Databricks. |
| Model UDF environment error | Ensure sklearn, NumPy and MLflow dependencies match the logged artifact. For a bounded example, use capstone local scoring; do not collect a large production table. |
| UC registration fails | Check catalog USE CATALOG, schema USE SCHEMA/CREATE MODEL and model ownership privileges. Ensure model signature and input example exist. |
| Endpoint missing / unauthorized | Pick an exact available endpoint name in Serving UI and confirm CAN QUERY. A repository connection does not provide Databricks endpoint access. |
| Tool calls unsupported | Choose a chat model endpoint with function calling support. Test the tool dispatcher independently first. |
| Invalid JSON or citations | Keep the failure visible. Validate outside the model and use bounded repair retries; do not silently invent successful output. |
| Search index not ready | Inspect the index and embedding endpoint status. Check source table permissions/CDF and rerun a triggered sync once ready. |
| Free Edition quota reached | Stop optional services and wait for the quota reset. Core CPU labs can run locally with the listed dependencies. |
| LoRA optional imports fail | Use an environment with compatible Torch/Transformers/PEFT. This lab is separate from the core dependency set. |
| Endpoint already exists | Inspect it in Serving UI. Choose a new course name or deliberately manage the existing course endpoint; do not overwrite a shared endpoint. |
| Unexpected results after package changes | Restart Python and rerun the entire notebook. Record package versions and compare against the validation report. |

## Suggested job layout

Use separate ingestion, training, validation and inference tasks. Jobs do not share notebook variables. Pass a run manifest containing source table version, run ID and immutable model URI through task values or a Delta table. Set task timeouts and retries only for operations designed to be idempotent. Keep final model promotion conditional on validation gates.

## Training versus production

Synthetic labels, tiny text corpora, fixed seeds and short loops make learning accessible. They do not test real customer behavior, hallucination rates, production throughput, privacy guarantees or model robustness. Extend the evaluation before applying these patterns to operational data.
