"""Silver job for Wikimedia recent-change events."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, get_json_object, to_timestamp


def run(args: argparse.Namespace) -> None:
    spark = SparkSession.builder.appName("streaming-api-silver-wikimedia").getOrCreate()

    bronze = spark.readStream.format("delta").load(args.bronze_path)

    silver = (
        bronze.filter(col("source_api") == "wikimedia_eventstreams")
        .select(
            col("message_key").alias("change_id"),
            to_timestamp("event_time").alias("event_timestamp"),
            get_json_object("payload", "$.wiki").alias("wiki"),
            get_json_object("payload", "$.type").alias("change_type"),
            get_json_object("payload", "$.title").alias("page_title"),
            get_json_object("payload", "$.user").alias("user_name"),
            get_json_object("payload", "$.bot").cast("boolean").alias("is_bot"),
            col("raw_json"),
            current_timestamp().alias("silver_loaded_at"),
        )
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicates(["change_id"])
    )

    (
        silver.writeStream.format("delta")
        .option("checkpointLocation", args.checkpoint_path)
        .outputMode("append")
        .start(args.output_path)
        .awaitTermination()
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-path", required=True)
    parser.add_argument("--checkpoint-path", required=True)
    parser.add_argument("--output-path", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
