# Validation report

Validated on 29 October 2026 with the checked-in `tests/validate_notebooks.py` script.

| Check | Result |
| --- | --- |
| Notebook JSON structure, unique cell IDs and manifest consistency | 30/30 passed |
| Python syntax for every code cell | 30/30 passed |
| Portable CPU notebooks, executed cell by cell in fresh namespaces | 22/22 passed |
| Optional notebook default paths, with cloud/training switches disabled | 3/3 passed |
| Failures in final local validation run | 0 |
| Databricks-specific notebook runtime execution | Not performed; 5 notebooks syntax checked only |
| Live GenAI, embeddings, managed search and model serving | Not performed |
| Optional PyTorch/PEFT training | Not performed |

Core dependency versions: NumPy 2.3.5, SciPy 1.17.0, scikit-learn 1.8.0. The local interpreter is Python 3.12. These results are not a test of every version in the documented supported ranges.

Assertions check numerical gradients, convergence, output shapes, train/test metrics, deterministic inference, malformed output rejection, retrieval rankings, citation IDs, tool validation, tenant isolation and bounded retries. The notebooks are stored without execution outputs to avoid implying they were run in your Databricks workspace.

The machine-readable [validation_results.json](validation_results.json) records each executed notebook and the exact runtime-exclusion list. Dry-run passes show that disabled defaults execute cleanly; they do not demonstrate that optional services, permissions, remote APIs or training work in a particular workspace.

## Remaining workspace validation

1. Configure notebook environments using lab 00 and select a writable catalog.
2. Run Spark/MLflow labs 03, 12, 13, 14 and 27 on your own workspace.
3. After reviewing quota and permissions, opt into model registration, serving, managed search, live inference or LoRA individually.
4. Record the workspace environment/package versions and retain the real run IDs and service responses with your study notes.

No Databricks workspace connection was available during course creation. GitHub access alone does not grant Spark compute, Unity Catalog, Managed MLflow or endpoint access.
