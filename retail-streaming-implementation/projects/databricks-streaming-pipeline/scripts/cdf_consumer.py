"""Optional external CDF audit sink; run as a separate Spark job, never inside dp definitions."""
import argparse
from pyspark.sql import SparkSession

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--source-table", required=True)
    p.add_argument("--target-table", required=True)
    p.add_argument("--checkpoint", required=True, help="Stable unique UC Volume path")
    p.add_argument("--starting-version", type=int, required=True)
    a = p.parse_args()
    spark = SparkSession.builder.getOrCreate()
    query = (spark.readStream.format("delta").option("readChangeFeed", "true")
        .option("startingVersion", a.starting_version).table(a.source_table)
        .writeStream.format("delta").outputMode("append")
        .option("checkpointLocation", a.checkpoint).trigger(availableNow=True).toTable(a.target_table))
    query.awaitTermination()
