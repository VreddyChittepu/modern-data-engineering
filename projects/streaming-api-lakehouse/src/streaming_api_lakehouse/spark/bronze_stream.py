"""Bronze Spark Structured Streaming job for raw API events."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, get_json_object


def run(args: argparse.Namespace) -> None:
    spark = SparkSession.builder.appName("streaming-api-bronze").getOrCreate()

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .load()
    )

    bronze = (
        raw.selectExpr("CAST(key AS STRING) AS message_key", "CAST(value AS STRING) AS raw_json", "timestamp AS kafka_timestamp")
        .withColumn("source_api", get_json_object("raw_json", "$.source_api"))
        .withColumn("source_domain", get_json_object("raw_json", "$.source_domain"))
        .withColumn("event_type", get_json_object("raw_json", "$.event_type"))
        .withColumn("event_time", get_json_object("raw_json", "$.event_time"))
        .withColumn("ingestion_time", get_json_object("raw_json", "$.ingestion_time"))
        .withColumn("payload", get_json_object("raw_json", "$.payload"))
        .withColumn("metadata", get_json_object("raw_json", "$.metadata"))
        .withColumn("bronze_loaded_at", current_timestamp())
    )

    (
        bronze.writeStream.format("delta")
        .option("checkpointLocation", args.checkpoint_path)
        .outputMode("append")
        .start(args.output_path)
        .awaitTermination()
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", default="api.raw.events")
    parser.add_argument("--starting-offsets", default="latest")
    parser.add_argument("--checkpoint-path", required=True)
    parser.add_argument("--output-path", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
