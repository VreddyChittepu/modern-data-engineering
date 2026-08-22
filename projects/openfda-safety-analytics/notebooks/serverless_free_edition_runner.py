# Databricks notebook source
# MAGIC %md
# MAGIC # openFDA Safety Analytics - Free Edition Runner
# MAGIC
# MAGIC This notebook runs on Databricks Free Edition serverless compute.
# MAGIC It tries live openFDA API calls and falls back to sample records if outbound API access is blocked.

# COMMAND ----------

from pathlib import Path
import json
from datetime import datetime, timezone

import requests
from pyspark.sql.functions import col, coalesce, current_timestamp, explode, to_date
from pyspark.sql.types import ArrayType, BooleanType, StringType, StructField, StructType

# COMMAND ----------

repo_root = Path.cwd()
while repo_root.name != "modern-data-engineering" and repo_root.parent != repo_root:
    repo_root = repo_root.parent

project_root = repo_root / "projects" / "openfda-safety-analytics"
catalog_path = project_root / "configs" / "openfda_endpoints.json"
catalog = json.loads(catalog_path.read_text())

display(spark.createDataFrame(catalog["endpoints"]))

# COMMAND ----------

base_path = "dbfs:/tmp/openfda-safety-analytics"
raw_path = f"{base_path}/raw-json"
bronze_path = f"{base_path}/delta/bronze_openfda_events"
silver_path = f"{base_path}/delta/silver_openfda_events"
gold_daily_path = f"{base_path}/delta/gold_daily_event_counts"
gold_reaction_path = f"{base_path}/delta/gold_top_reactions"

dbutils.fs.rm(base_path, recurse=True)
dbutils.fs.mkdirs(raw_path)

# COMMAND ----------

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def sample_records(endpoint_name):
    return [
        {
            "endpoint_name": endpoint_name,
            "source_url": "https://api.fda.gov",
            "ingestion_time": utc_now_iso(),
            "raw_record": {
                "safetyreportid": f"{endpoint_name}-sample-1",
                "mdr_report_key": f"{endpoint_name}-device-sample-1",
                "report_number": f"{endpoint_name}-food-sample-1",
                "receivedate": "20260110",
                "date_received": "2026-01-10",
                "date_started": "20260110",
                "serious": "1",
                "patient": {
                    "reaction": [{"reactionmeddrapt": "Nausea"}, {"reactionmeddrapt": "Headache"}],
                    "drug": [{"medicinalproduct": "Sample Product A"}],
                },
            },
        },
        {
            "endpoint_name": endpoint_name,
            "source_url": "https://api.fda.gov",
            "ingestion_time": utc_now_iso(),
            "raw_record": {
                "safetyreportid": f"{endpoint_name}-sample-2",
                "mdr_report_key": f"{endpoint_name}-device-sample-2",
                "report_number": f"{endpoint_name}-food-sample-2",
                "receivedate": "20260111",
                "date_received": "2026-01-11",
                "date_started": "20260111",
                "serious": "0",
                "patient": {
                    "reaction": [{"reactionmeddrapt": "Dizziness"}],
                    "drug": [{"medicinalproduct": "Sample Product B"}],
                },
            },
        },
    ]


def fetch_endpoint(endpoint):
    url = catalog["base_url"].rstrip("/") + endpoint["path"]
    params = {"limit": 25}
    api_key = dbutils.secrets.get("openfda", "api_key") if False else None
    if api_key:
        params["api_key"] = api_key

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()
    records = payload.get("results", [])
    return [
        {
            "endpoint_name": endpoint["name"],
            "source_url": url,
            "ingestion_time": utc_now_iso(),
            "raw_record": record,
        }
        for record in records
    ]


all_records = []
for endpoint in catalog["endpoints"]:
    try:
        all_records.extend(fetch_endpoint(endpoint))
    except Exception as exc:
        print(f"Using sample records for {endpoint['name']}: {exc}")
        all_records.extend(sample_records(endpoint["name"]))

local_file = "/tmp/openfda_adverse_events.json"
with open(local_file, "w", encoding="utf-8") as handle:
    for record in all_records:
        handle.write(json.dumps(record) + "\n")

dbutils.fs.cp(f"file:{local_file}", f"{raw_path}/openfda_events.json")
print(f"Wrote {len(all_records)} records")

# COMMAND ----------

bronze_schema = StructType(
    [
        StructField("endpoint_name", StringType()),
        StructField("source_url", StringType()),
        StructField("ingestion_time", StringType()),
        StructField("raw_record", StringType()),
    ]
)

bronze_df = (
    spark.read.json(raw_path)
    .withColumn("bronze_loaded_at", current_timestamp())
)

bronze_df.write.format("delta").mode("overwrite").save(bronze_path)
display(spark.read.format("delta").load(bronze_path))

# COMMAND ----------

reaction_schema = ArrayType(StructType([StructField("reactionmeddrapt", StringType())]))
drug_schema = ArrayType(StructType([StructField("medicinalproduct", StringType())]))

silver_df = (
    spark.read.format("delta").load(bronze_path)
    .select(
        col("endpoint_name"),
        col("source_url"),
        col("ingestion_time"),
        coalesce(
            col("raw_record.safetyreportid"),
            col("raw_record.mdr_report_key"),
            col("raw_record.report_number"),
        ).alias("event_id"),
        coalesce(
            to_date(col("raw_record.receivedate"), "yyyyMMdd"),
            to_date(col("raw_record.date_received"), "yyyy-MM-dd"),
            to_date(col("raw_record.date_started"), "yyyyMMdd"),
        ).alias("event_date"),
        col("raw_record.serious").cast("boolean").alias("is_serious"),
        col("raw_record.patient.reaction").alias("reactions"),
        col("raw_record.patient.drug").alias("products"),
        col("raw_record").alias("raw_record"),
    )
    .withColumn("silver_loaded_at", current_timestamp())
)

silver_df.write.format("delta").mode("overwrite").save(silver_path)
display(spark.read.format("delta").load(silver_path))

# COMMAND ----------

daily_counts = (
    spark.read.format("delta").load(silver_path)
    .groupBy("event_date", "endpoint_name")
    .count()
    .withColumnRenamed("count", "event_count")
)

daily_counts.write.format("delta").mode("overwrite").save(gold_daily_path)
display(daily_counts.orderBy("event_date", "endpoint_name"))

# COMMAND ----------

top_reactions = (
    spark.read.format("delta").load(silver_path)
    .select("endpoint_name", explode("reactions").alias("reaction"))
    .select("endpoint_name", col("reaction.reactionmeddrapt").alias("reaction_term"))
    .where(col("reaction_term").isNotNull())
    .groupBy("endpoint_name", "reaction_term")
    .count()
    .withColumnRenamed("count", "reaction_count")
)

top_reactions.write.format("delta").mode("overwrite").save(gold_reaction_path)
display(top_reactions.orderBy(col("reaction_count").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output Delta Paths
# MAGIC
# MAGIC - `dbfs:/tmp/openfda-safety-analytics/delta/bronze_openfda_events`
# MAGIC - `dbfs:/tmp/openfda-safety-analytics/delta/silver_openfda_events`
# MAGIC - `dbfs:/tmp/openfda-safety-analytics/delta/gold_daily_event_counts`
# MAGIC - `dbfs:/tmp/openfda-safety-analytics/delta/gold_top_reactions`
