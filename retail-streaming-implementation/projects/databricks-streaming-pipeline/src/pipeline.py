"""Run only inside a Databricks Lakeflow Spark Declarative Pipeline.
No writes/actions inside dataset definitions; Lakeflow owns orchestration and checkpoints.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window, SparkSession
from retail.config import Config
from retail.transforms import (parse_events, validate, validation_rules, deduplicate,
    customer_changes, order_changes, flatten_items, sales_summary, window_metrics)

spark = SparkSession.getActiveSession()
cfg = Config.from_spark(spark)
t = cfg.table
CDF = {"delta.enableChangeDataFeed": "true"}

@dp.table(name=t("bronze", "raw_events"), table_properties={"delta.appendOnly": "true"})
def raw_events():
    # Text is intentional: malformed JSON and exact payload text survive ingestion.
    return (spark.readStream.format("cloudFiles").option("cloudFiles.format", "text")
        .option("cloudFiles.maxFilesPerTrigger", "1000").load(cfg.source_path)
        .select(F.col("value").alias("raw_payload"),
            F.col("_metadata.file_path").alias("source_file"),
            F.col("_metadata.file_modification_time").alias("source_file_modification_timestamp"),
            F.current_timestamp().alias("ingestion_timestamp"))
        .withColumn("processing_timestamp", F.current_timestamp())
        .withColumn("event_id", F.get_json_object("raw_payload", "$.event_id"))
        .withColumn("event_type", F.get_json_object("raw_payload", "$.event_type"))
        .withColumn("event_timestamp", F.get_json_object("raw_payload", "$.event_timestamp"))
        .withColumn("rescued_data", F.when(F.get_json_object("raw_payload", "$").isNull(), F.col("raw_payload")))
        .withColumn("pipeline_contract_version", F.lit("3")))

@dp.table(name=t("bronze", "schema_probe"))
def schema_probe():
    # Independent diagnostic ingestion: genuine Auto Loader rescue of new fields/type mismatches.
    # It is not joined to raw_events and never controls business record counts.
    return (spark.readStream.format("cloudFiles").option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", cfg.schema_location)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.schemaHints", "event_id STRING, event_timestamp STRING")
        .option("rescuedDataColumn", "_rescued_data").option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(cfg.source_path).select("*", F.col("_metadata.file_path").alias("source_file")))

@dp.table(name=t("dq", "validated_events"))
@dp.expect_all(validation_rules())
def validated_events():
    return validate(parse_events(spark.readStream.table(t("bronze", "raw_events"))), cfg.lateness_seconds)

@dp.table(name=t("dq", "invalid_events"))
def invalid_events():
    return spark.readStream.table(t("dq", "validated_events")).filter("NOT is_valid")

@dp.table(name=t("dq", "late_events"))
def late_events():
    # Arrival SLA audit, NOT a claim that Spark dropped this row at its current watermark.
    return spark.readStream.table(t("dq", "validated_events")).filter("is_valid AND is_late_by_sla")

@dp.table(name=t("silver", "events"), table_properties=CDF)
def silver_events():
    return deduplicate(spark.readStream.table(t("dq", "validated_events")).filter("is_valid"), cfg.watermark)

@dp.table(name=t("silver", "customer_changes"))
def customer_cdc_source():
    # CDC bypasses event-time watermark: old business updates must reach sequence-aware AUTO CDC.
    return customer_changes(spark.readStream.table(t("dq", "validated_events")).filter("is_valid"))

@dp.table(name=t("silver", "order_changes"))
def order_cdc_source():
    return order_changes(spark.readStream.table(t("dq", "validated_events")).filter("is_valid"))

for target, source, key, scd in [
    ("customers", "customer_changes", "customer_id", 1),
    ("customer_history", "customer_changes", "customer_id", 2),
    ("orders", "order_changes", "order_id", 1),
]:
    dp.create_streaming_table(name=t("silver", target), table_properties={**CDF,
        "pipelines.cdc.tombstoneGCThresholdInSeconds": str(cfg.tombstone_seconds)})
    dp.create_auto_cdc_flow(target=t("silver", target), source=t("silver", source), keys=[key],
        sequence_by=F.struct("sequence_number", "event_timestamp", "event_id", "payload_hash"),
        apply_as_deletes=F.expr("operation='DELETE'"), stored_as_scd_type=scd,
        ignore_null_updates=False)

@dp.materialized_view(name=t("silver", "customer_addresses"))
def customer_addresses():
    return spark.read.table(t("silver", "customers")).select("customer_id",
        F.posexplode("addresses").alias("address_index", "address")).select("customer_id", "address_index", "address.*")

@dp.materialized_view(name=t("silver", "order_items"))
def current_order_items():
    return flatten_items(spark.read.table(t("silver", "orders")).filter("order_status <> 'CANCELLED'"))

@dp.table(name=t("silver", "order_created_facts"))
def order_created_facts():
    return order_changes(spark.readStream.table(t("silver", "events")).filter("event_type='ORDER_CREATED'"))

@dp.table(name=t("silver", "order_item_events"))
def order_item_events():
    return flatten_items(spark.readStream.table(t("silver", "order_created_facts")))

@dp.table(name=t("silver", "payments"))
def payments():
    return (spark.readStream.table(t("silver", "events")).filter("event_type='PAYMENT_COMPLETED'")
        .select("event_id", "event_timestamp", "order.order_id", "order.currency", "order.payment.*")
        .withColumn("amount", F.expr("try_cast(amount as decimal(18,2))")))

@dp.table(name=t("silver", "shipments"))
def shipments():
    return (spark.readStream.table(t("silver", "events")).filter("event_type='SHIPMENT_CREATED'")
        .select("event_id", "event_timestamp", "order.order_id", "order.shipping.*"))

@dp.materialized_view(name=t("gold", "customer_history"))
def scd2_business_projection():
    h = spark.read.table(t("silver", "customer_history"))
    return (h.withColumn("effective_start_date", F.col("__START_AT.event_timestamp"))
        .withColumn("effective_end_date", F.col("__END_AT.event_timestamp"))
        .withColumn("is_current", F.col("__END_AT").isNull())
        .withColumn("record_version", F.row_number().over(Window.partitionBy("customer_id").orderBy("__START_AT")))
        .withColumnRenamed("event_timestamp", "source_event_timestamp"))

@dp.materialized_view(name=t("gold", "customer_360"))
def customer_360():
    # Grain customer + currency; never add INR to USD. Deleted customers disappear by inner ownership.
    o = spark.read.table(t("silver", "orders")).filter("order_status <> 'CANCELLED'")
    totals = o.groupBy("customer_id", "currency").agg(F.count("order_id").alias("lifetime_orders"),
        F.sum("order_amount").alias("lifetime_revenue"), F.max("created_at").alias("last_order_timestamp"))
    return (spark.read.table(t("silver", "customers")).join(totals, "customer_id", "left")
        .fillna({"lifetime_orders": 0, "lifetime_revenue": 0}).withColumn("customer_status", F.lit("ACTIVE")))

@dp.materialized_view(name=t("gold", "sales_summary"))
def gold_sales_summary():
    return sales_summary(spark.read.table(t("silver", "order_items")))

@dp.materialized_view(name=t("gold", "product_performance"))
def product_performance():
    return (spark.read.table(t("silver", "order_items")).groupBy("product_id", "product_name", "category", "currency")
        .agg(F.sum("quantity").alias("units_sold"), F.sum("line_revenue").alias("revenue"),
             F.countDistinct("customer_id").alias("unique_customers")))

# Each operator starts from a persisted append-only fact table; no stream reads of mutable CDC tables.
def register_window(kind):
    @dp.table(name=t("gold", f"sales_{kind}"))
    def metrics():
        return window_metrics(spark.readStream.table(t("silver", "order_created_facts")), cfg.watermark, kind)
for kind in ("tumbling", "sliding", "session"):
    register_window(kind)

@dp.materialized_view(name=t("gold", "streaming_dashboard"))
def dashboard():
    # Fresh provisional metrics; streaming window tables only emit finalized windows.
    return (spark.read.table(t("silver", "order_created_facts"))
        .groupBy(F.window("event_timestamp", "10 minutes"), "region", "currency")
        .agg(F.count("order_id").alias("order_count"), F.sum("order_amount").alias("revenue"))
        .withColumn("average_order_value", F.col("revenue") / F.col("order_count")))

@dp.materialized_view(name=t("dq", "data_quality_metrics"))
def quality_metrics():
    return (spark.read.table(t("dq", "validated_events"))
        .groupBy(F.to_date("ingestion_timestamp").alias("ingestion_date"), "event_type", "operation")
        .agg(F.count("*").alias("records_processed"),
             F.sum((~F.col("is_valid")).cast("long")).alias("records_rejected"),
             F.sum(F.col("is_late_by_sla").cast("long")).alias("late_by_sla"),
             F.avg("arrival_delay_seconds").alias("average_arrival_delay_seconds")))

@dp.materialized_view(name=t("dq", "duplicate_audit"))
def duplicate_audit():
    return (spark.read.table(t("dq", "validated_events")).filter("event_id IS NOT NULL")
        .groupBy("event_id").agg(F.count("*").alias("copies"),
            F.countDistinct("payload_hash").alias("payload_variants"))
        .filter("copies>1").withColumn("duplicate_count", F.col("copies") - 1))

@dp.materialized_view(name=t("dq", "cdc_conflicts"))
def cdc_conflicts():
    return (spark.read.table(t("dq", "validated_events")).filter("is_valid AND entity IS NOT NULL")
        .withColumn("business_key", F.when(F.col("entity")=="customer", F.col("customer.customer_id"))
            .otherwise(F.col("order.order_id")))
        .groupBy("entity", "business_key", "sequence_number")
        .agg(F.countDistinct("payload_hash").alias("payload_variants"), F.collect_set("event_id").alias("event_ids"))
        .filter("payload_variants>1"))
