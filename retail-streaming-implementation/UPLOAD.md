# Upload the implementation

The archive contains repository-relative source files and a Git patch of the same change.
Use either the patch OR copy the source files; do not apply both.

From an authenticated local clone of VreddyChittepu/modern-data-engineering:

```bash
git switch -c feat/retail-databricks-streaming
git am /absolute/path/to/retail-streaming.patch
git push -u origin feat/retail-databricks-streaming
```

Then open a pull request into main. Write access is required.
The code is not yet uploaded or deployed. All 10 local tests passed.
Project setup and Databricks runtime acceptance gates are in
projects/databricks-streaming-pipeline/README.md.
