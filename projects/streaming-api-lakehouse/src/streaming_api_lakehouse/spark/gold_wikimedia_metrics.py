"""Gold aggregates for Wikimedia recent-change activity."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, window


def run(args: argparse.Namespace) -> None:
    spark = SparkSession.builder.appName("streaming-api-gold-wikimedia").getOrCreate()

    silver = spark.readStream.format("delta").load(args.silver_path)

    gold = (
        silver.withWatermark("event_timestamp", "10 minutes")
        .groupBy(window("event_timestamp", "1 minute"), "wiki", "change_type", "is_bot")
        .agg(count("*").alias("change_count"))
    )

    (
        gold.writeStream.format("delta")
        .option("checkpointLocation", args.checkpoint_path)
        .outputMode("append")
        .start(args.output_path)
        .awaitTermination()
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--checkpoint-path", required=True)
    parser.add_argument("--output-path", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
