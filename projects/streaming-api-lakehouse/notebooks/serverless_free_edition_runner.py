# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming API Lakehouse - Databricks Free Edition Runner
# MAGIC
# MAGIC This notebook is designed for Databricks Free Edition with serverless compute.
# MAGIC It avoids Kafka and long-running streaming triggers, then uses `Trigger.AvailableNow()`
# MAGIC so the workload is compatible with serverless notebooks.

# COMMAND ----------

from pathlib import Path
import json
import time
from datetime import datetime, timezone
from random import choice, randint, random

from pyspark.sql.functions import col, current_timestamp, from_json, to_timestamp, window
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

# COMMAND ----------

# Update this only if your repo folder name/path is different.
repo_root = Path.cwd()
while repo_root.name != "modern-data-engineering" and repo_root.parent != repo_root:
    repo_root = repo_root.parent

project_root = repo_root / "projects" / "streaming-api-lakehouse"
catalog_path = project_root / "configs" / "streaming_apis.json"

print(f"Repo root: {repo_root}")
print(f"Project root: {project_root}")
print(f"Catalog path: {catalog_path}")

# COMMAND ----------

catalog = json.loads(catalog_path.read_text())
streaming_api_names = [api["name"] for api in catalog["apis"]]

display(spark.createDataFrame(catalog["apis"]))

# COMMAND ----------

base_path = "dbfs:/tmp/streaming-api-lakehouse-free-edition"
raw_json_path = f"{base_path}/raw-json"
bronze_path = f"{base_path}/delta/bronze_api_events"
silver_path = f"{base_path}/delta/silver_wikimedia_recentchange"
gold_path = f"{base_path}/delta/gold_wikimedia_metrics"

bronze_checkpoint = f"{base_path}/checkpoints/bronze"
silver_checkpoint = f"{base_path}/checkpoints/silver"
gold_checkpoint = f"{base_path}/checkpoints/gold"

print(raw_json_path)

# COMMAND ----------

# Optional reset for repeatable practice runs.
dbutils.fs.rm(base_path, recurse=True)
dbutils.fs.mkdirs(raw_json_path)

# COMMAND ----------

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def build_sample_wikimedia_event(i):
    wiki = choice(["enwiki", "commonswiki", "wikidatawiki", "dewiki", "frwiki"])
    change_type = choice(["edit", "new", "log", "categorize"])
    is_bot = random() < 0.2
    event_time = utc_now_iso()
    return {
        "source_api": "wikimedia_eventstreams",
        "source_domain": "Streaming / Media",
        "event_type": "recentchange",
        "event_time": event_time,
        "ingestion_time": event_time,
        "payload": {
            "id": i,
            "wiki": wiki,
            "type": change_type,
            "title": f"Sample_Page_{randint(1, 50)}",
            "user": choice(["alice", "bob", "carol", "demo_bot"]),
            "bot": is_bot,
        },
        "metadata": {
            "runner": "databricks_free_edition",
            "source_note": "synthetic event shaped like Wikimedia EventStreams recentchange",
        },
    }


sample_events = [build_sample_wikimedia_event(i) for i in range(1, 501)]
local_file = "/tmp/wikimedia_recentchange_sample.json"

with open(local_file, "w", encoding="utf-8") as handle:
    for event in sample_events:
        handle.write(json.dumps(event) + "\n")

dbutils.fs.cp(f"file:{local_file}", f"{raw_json_path}/batch_{int(time.time())}.json")
print(f"Wrote {len(sample_events)} sample events to {raw_json_path}")

# COMMAND ----------

raw_schema = StructType(
    [
        StructField("source_api", StringType()),
        StructField("source_domain", StringType()),
        StructField("event_type", StringType()),
        StructField("event_time", StringType()),
        StructField("ingestion_time", StringType()),
        StructField(
            "payload",
            StructType(
                [
                    StructField("id", StringType()),
                    StructField("wiki", StringType()),
                    StructField("type", StringType()),
                    StructField("title", StringType()),
                    StructField("user", StringType()),
                    StructField("bot", BooleanType()),
                ]
            ),
        ),
        StructField(
            "metadata",
            StructType(
                [
                    StructField("runner", StringType()),
                    StructField("source_note", StringType()),
                ]
            ),
        ),
    ]
)

bronze_stream = (
    spark.readStream.format("json")
    .schema(raw_schema)
    .load(raw_json_path)
    .withColumn("bronze_loaded_at", current_timestamp())
)

(
    bronze_stream.writeStream.format("delta")
    .option("checkpointLocation", bronze_checkpoint)
    .trigger(availableNow=True)
    .outputMode("append")
    .start(bronze_path)
    .awaitTermination()
)

print(f"Bronze complete: {bronze_path}")

# COMMAND ----------

bronze_df = spark.read.format("delta").load(bronze_path)
display(bronze_df)

# COMMAND ----------

silver_stream = (
    spark.readStream.format("delta")
    .load(bronze_path)
    .filter(col("source_api") == "wikimedia_eventstreams")
    .select(
        col("payload.id").alias("change_id"),
        to_timestamp("event_time").alias("event_timestamp"),
        col("payload.wiki").alias("wiki"),
        col("payload.type").alias("change_type"),
        col("payload.title").alias("page_title"),
        col("payload.user").alias("user_name"),
        col("payload.bot").alias("is_bot"),
        col("source_api"),
        current_timestamp().alias("silver_loaded_at"),
    )
    .withWatermark("event_timestamp", "10 minutes")
    .dropDuplicates(["change_id"])
)

(
    silver_stream.writeStream.format("delta")
    .option("checkpointLocation", silver_checkpoint)
    .trigger(availableNow=True)
    .outputMode("append")
    .start(silver_path)
    .awaitTermination()
)

print(f"Silver complete: {silver_path}")

# COMMAND ----------

silver_df = spark.read.format("delta").load(silver_path)
display(silver_df)

# COMMAND ----------

gold_stream = (
    spark.readStream.format("delta")
    .load(silver_path)
    .withWatermark("event_timestamp", "10 minutes")
    .groupBy(window("event_timestamp", "1 minute"), "wiki", "change_type", "is_bot")
    .count()
    .withColumnRenamed("count", "change_count")
)

(
    gold_stream.writeStream.format("delta")
    .option("checkpointLocation", gold_checkpoint)
    .trigger(availableNow=True)
    .outputMode("append")
    .start(gold_path)
    .awaitTermination()
)

print(f"Gold complete: {gold_path}")

# COMMAND ----------

gold_df = spark.read.format("delta").load(gold_path)
display(gold_df.orderBy("window", "wiki", "change_type"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: register tables
# MAGIC
# MAGIC If you want SQL tables, uncomment and run the next cell. In Free Edition,
# MAGIC use a catalog/schema you have permission to write to.

# COMMAND ----------

# spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.default")
# spark.sql(f"CREATE TABLE IF NOT EXISTS workspace.default.bronze_api_events USING DELTA LOCATION '{bronze_path}'")
# spark.sql(f"CREATE TABLE IF NOT EXISTS workspace.default.silver_wikimedia_recentchange USING DELTA LOCATION '{silver_path}'")
# spark.sql(f"CREATE TABLE IF NOT EXISTS workspace.default.gold_wikimedia_metrics USING DELTA LOCATION '{gold_path}'")
