# Databricks PySpark scenario notebooks

**50 independent, step-by-step notebooks** covering major day-to-day Databricks/PySpark scenarios, from DataFrame fundamentals to Delta, streaming, governance, performance and enterprise integration. Each includes a real asset-backed example, explained steps, expected checks, a practice task and solution guidance. No finite course covers every API, connector or runtime-specific behavior; this is a broad practical core with the coverage and boundaries listed below.

## Dataset rule

All scenario record data comes from **Databricks-provided `samples.nyctaxi.trips`**. This is a built-in Unity Catalog sample asset. Source fields are pickup/dropoff timestamps, distance, fare and pickup/dropoff ZIP. There are no synthetic record fixtures, random/generated datasets, sklearn datasets, external downloads or pandas conversions.

Nested objects, replay batches, effective-dated histories, exported CSV/JSON/Parquet, streaming sources and optional DB2/JDBC exports are transformations or copies of these asset records. The `samples` asset is never modified. Fixed strings such as metric labels, configuration metadata and outcome reasons are application metadata, not replacement datasets. If the asset cannot be accessed, the notebooks stop clearly; there is no dataset fallback. An optional listing in lab 00 shows the official `/Volumes/samples/databricks/datasets/` collection without reading a different dataset.

## Run in Databricks

1. Add `VreddyChittepu/modern-data-engineering` as a Git folder, or import the `.ipynb` notebooks into Workspace.
2. Open [00 — Setup](notebooks/00_setup_assets_and_compute.ipynb). Attach recent Databricks Python compute with access to the `samples` catalog. Use a recent LTS/runtime environment (Spark 3.5+ for watermark dedup; feature-specific minimum versions vary).
3. Run top to bottom. `max_rows` defaults to **2,000**, allowed range 100–20,000. Every notebook loads its own asset slice and does not depend on variables from another notebook.
4. For write labs, set `target_catalog` to a writable catalog. The lab creates a per-user `pyspark_lab_<hash>` schema; its output names include the lab number. It will never write to `samples`.
5. File/streaming labs also need CREATE VOLUME and volume read/write privileges. They use unique run directories/checkpoints and per-run stream table names to avoid mixing a fresh checkpoint with an existing sink.
6. The DB2/JDBC/RDD blocks and optional mutations have explicit switches off by default. Review their prerequisites before enabling them.

## Suggested learning sequence

- **00–05:** setup, schemas, expressions, nulls, strings, dates.
- **06–19:** aggregations, joins, windows, sessions, nested data, JSON, sets, statistics and SQL.
- **20–21:** reusable transformations and native functions versus UDFs.
- **22–29:** Delta, schemas, history, SCD1/SCD2, CDF, incremental ingestion and medallion capstone.
- **30–35:** file formats, Auto Loader, checkpoints, watermarks, foreachBatch and stream-stream joins.
- **36–41:** plans, broadcast/AQE, partitions, cache, layout, governance and job parameters.
- **42–49:** optional JDBC/DB2 integration, asset-backed tests, reconciliation, metadata-driven ETL, observability, Spark ML and legacy partition APIs.

## Coverage map

| Lab | Scenarios | Mode | Course writes |
| --- | --- | --- | --- |
| 00 | [Setup, Databricks assets and compute](notebooks/00_setup_assets_and_compute.ipynb) | dataframe | no |
| 01 | [Schemas, types, renaming and column selection](notebooks/01_schemas_types_and_columns.ipynb) | dataframe | no |
| 02 | [Select, filter, CASE and sorting](notebooks/02_select_filter_case_and_sort.ipynb) | dataframe | no |
| 03 | [NULL, NaN, safe casts and data-quality quarantine](notebooks/03_nulls_nan_and_safe_casts.ipynb) | dataframe | no |
| 04 | [Strings, regex, splitting and leading zeros](notebooks/04_strings_regex_and_numeric_identifiers.ipynb) | dataframe | no |
| 05 | [Dates, timestamps, intervals and timezone semantics](notebooks/05_dates_timestamps_and_timezones.ipynb) | dataframe | no |
| 06 | [GroupBy, aggregate functions and distinct counts](notebooks/06_groupby_aggregations_and_distinct.ipynb) | dataframe | no |
| 07 | [Inner, outer, semi, anti and cross joins](notebooks/07_join_types_and_cardinality.ipynb) | dataframe | no |
| 08 | [Null-safe equality, range joins and skew inspection](notebooks/08_null_safe_and_range_joins.ipynb) | dataframe | no |
| 09 | [row_number, rank, dense_rank and deterministic deduplication](notebooks/09_ranking_and_deduplication.ipynb) | dataframe | no |
| 10 | [lag, lead, running totals and moving averages](notebooks/10_window_lag_lead_running_totals.ipynb) | dataframe | no |
| 11 | [Sessionization and gaps-and-islands](notebooks/11_sessions_and_gaps_islands.ipynb) | dataframe | no |
| 12 | [Structs, arrays, explode and flatten](notebooks/12_struct_array_explode_and_flatten.ipynb) | dataframe | no |
| 13 | [Maps and higher-order array functions](notebooks/13_maps_and_higher_order_functions.ipynb) | dataframe | no |
| 14 | [JSON parsing, schema enforcement and flattening](notebooks/14_json_parsing_and_nested_payloads.ipynb) | dataframe | no |
| 15 | [Pivot and unpivot](notebooks/15_pivot_and_unpivot.ipynb) | dataframe | no |
| 16 | [unionByName, exceptAll and intersect](notebooks/16_unions_and_set_operations.ipynb) | dataframe | no |
| 17 | [Statistics, quantiles and outlier profiling](notebooks/17_statistics_quantiles_and_outliers.ipynb) | dataframe | no |
| 18 | [Rollup, cube and subtotal rows](notebooks/18_rollup_cube_and_grouping_sets.ipynb) | dataframe | no |
| 19 | [SQL views and safe parameter binding](notebooks/19_sql_views_and_parameter_markers.ipynb) | dataframe | no |
| 20 | [Reusable functions, DataFrame.transform and classes](notebooks/20_reusable_functions_and_classes.ipynb) | dataframe | no |
| 21 | [Native expressions versus Python UDFs](notebooks/21_native_functions_and_python_udfs.ipynb) | dataframe | no |
| 22 | [Delta CRUD and MERGE upserts](notebooks/22_delta_crud_and_merge.ipynb) | delta | yes |
| 23 | [Delta schema evolution and selective overwrite](notebooks/23_delta_schema_evolution.ipynb) | delta | yes |
| 24 | [Delta history, time travel and restore](notebooks/24_history_time_travel_and_restore.ipynb) | delta | yes |
| 25 | [SCD Type 1: latest state per business key](notebooks/25_scd_type1.ipynb) | delta | yes |
| 26 | [SCD Type 2: effective-dated history](notebooks/26_scd_type2_history.ipynb) | delta | yes |
| 27 | [Delta Change Data Feed](notebooks/27_change_data_feed.ipynb) | delta | yes |
| 28 | [Incremental batch loads and replay-safe upserts](notebooks/28_incremental_batch_and_watermarks.ipynb) | delta | yes |
| 29 | [Bronze → Silver → Gold ETL capstone](notebooks/29_bronze_silver_gold_capstone.ipynb) | delta | yes |
| 30 | [CSV, JSON and Parquet in Unity Catalog volumes](notebooks/30_csv_json_parquet_volume_roundtrips.ipynb) | volume | yes |
| 31 | [Auto Loader with schema tracking and AvailableNow](notebooks/31_auto_loader.ipynb) | streaming | yes |
| 32 | [Delta streaming and checkpoint restart](notebooks/32_delta_streaming_and_checkpoint_restart.ipynb) | streaming | yes |
| 33 | [Event-time watermarks and bounded-state deduplication](notebooks/33_streaming_watermark_and_dedup.ipynb) | streaming | yes |
| 34 | [foreachBatch with an idempotent Delta MERGE sink](notebooks/34_foreachbatch_idempotent_merge.ipynb) | streaming | yes |
| 35 | [Stream-stream joins with watermarks and time bounds](notebooks/35_stream_stream_time_bounded_join.ipynb) | streaming | yes |
| 36 | [Query plans, broadcast joins and AQE](notebooks/36_query_plans_broadcast_and_aqe.ipynb) | dataframe | no |
| 37 | [Repartition, coalesce and output file inspection](notebooks/37_partitions_shuffle_and_file_counts.ipynb) | volume | yes |
| 38 | [Caching, actions and driver-memory limits](notebooks/38_cache_persist_and_actions.ipynb) | dataframe | no |
| 39 | [OPTIMIZE and liquid clustering](notebooks/39_optimize_and_liquid_clustering.ipynb) | delta | yes |
| 40 | [Unity Catalog metadata, tables and lineage](notebooks/40_unity_catalog_metadata_and_lineage.ipynb) | delta | yes |
| 41 | [Widgets, job parameters and task values](notebooks/41_widgets_job_parameters_and_task_values.ipynb) | dataframe | no |
| 42 | [JDBC round trip and partitioned reads](notebooks/42_jdbc_roundtrip_and_partition_options.ipynb) | integration | no |
| 43 | [DB2 partition writes, batches of 200 and full-row rejects](notebooks/43_db2_partition_batches_and_rejects.ipynb) | classic-integration | yes |
| 44 | [Asset-backed transformation tests](notebooks/44_asset_backed_testing.ipynb) | dataframe | no |
| 45 | [Source-target reconciliation](notebooks/45_reconciliation_full_outer_hashes.ipynb) | dataframe | no |
| 46 | [Configuration-driven transformations](notebooks/46_metadata_driven_transforms.ipynb) | dataframe | no |
| 47 | [Data-quality metrics and run manifests](notebooks/47_observability_and_run_manifests.ipynb) | delta | yes |
| 48 | [Spark ML feature engineering and regression](notebooks/48_spark_ml_feature_pipeline.ipynb) | spark-ml | no |
| 49 | [Classic RDD map/filter/mapPartitions](notebooks/49_classic_rdd_and_partition_processing.ipynb) | classic | no |

## Compatibility and important limits

Core DataFrame/Delta/streaming labs target current Databricks APIs and Unity Catalog. Source/sample availability can vary. The suite does not install PySpark, pandas, local Spark or third-party datasets. Serverless/standard compute does not expose RDD APIs; use dedicated classic compute only for labs explicitly labeled classic. JDBC driver jars, executor JVM access, secret/network permissions and JayDeBeApi are additional requirements for optional integrations.

Some examples intentionally teach limitations: SCD2 is a **batch history rebuild**, not an incremental late-event MERGE implementation; exact-row trip_id is a **content fingerprint**, not a guaranteed business trip key; incremental overlap does not detect arbitrarily late corrections; streaming checkpoints do not deduplicate business records; Spark external writes are not exactly once. These boundaries are explained next to the relevant code.

The source asset timestamps have no timezone metadata in the six-field contract. Establish the actual source convention before converting wall times to UTC. Sorting/limiting bounds output size, not source scan cost. Small samples explain APIs and cannot prove production performance.

## Writes, reruns and cleanup

Delta write labs may overwrite their own lab-specific course tables. Stream/file labs create fresh run directories and stream table names; inside a checkpoint-restart exercise the checkpoint remains stable. For production, keep a stable source/sink/checkpoint identity and explicit recovery policy. Keep sufficient quota/storage for the lab outputs and clean up only the named course assets after review. No broad schema drops, credential commits, permission changes or unsafe VACUUM are executed.

## Validation and references

Run `python tests/validate_notebooks.py` locally for notebook structure, syntax, manifest, asset-provenance and optional-switch checks. This does not execute Spark. Run `python tests/validate_notebooks.py --help` for details. Runtime assertions inside every notebook execute against the real asset once run in Databricks. The optional integration defaults are not evidence that those integrations were tested.

See [validation](docs/VALIDATION.md), [compatibility and troubleshooting](docs/TROUBLESHOOTING.md), [official references](docs/REFERENCES.md), and the [SQL job-parameter template](sql/trips_for_date.sql).
