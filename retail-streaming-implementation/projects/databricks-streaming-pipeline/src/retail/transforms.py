from pyspark.sql import functions as F
from retail.schemas import EVENT, EVENT_TYPES


def parse_events(raw):
    parsed = raw.withColumn("payload", F.from_json("raw_payload", EVENT, {
        "mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}))
    return (parsed
        .withColumn("event_id", F.col("payload.event_id"))
        .withColumn("event_type", F.col("payload.event_type"))
        .withColumn("event_timestamp", F.expr("try_cast(payload.event_timestamp as timestamp)"))
        .withColumn("operation", F.upper(F.col("payload.cdc.operation")))
        .withColumn("entity", F.lower(F.col("payload.cdc.entity")))
        .withColumn("sequence_number", F.expr("try_cast(payload.cdc.sequence_number as bigint)"))
        .withColumn("customer", F.when(F.col("operation") == "DELETE",
            F.coalesce("payload.cdc.before.customer", "payload.customer"))
            .otherwise(F.coalesce("payload.cdc.after.customer", "payload.customer")))
        .withColumn("order", F.when(F.col("operation") == "DELETE",
            F.coalesce("payload.cdc.before.order", "payload.order"))
            .otherwise(F.coalesce("payload.cdc.after.order", "payload.order")))
        .withColumn("payload_hash", F.sha2("raw_payload", 256)))


def validation_rules():
    valid_types = ",".join(repr(t) for t in EVENT_TYPES)
    return {
        "json": "payload IS NOT NULL AND payload._corrupt_record IS NULL",
        "event_id": "event_id IS NOT NULL AND trim(event_id) <> ''",
        "event_type": f"event_type IN ({valid_types})",
        "timestamp": "event_timestamp IS NOT NULL AND event_timestamp >= TIMESTAMP '2020-01-01' "
                     "AND event_timestamp <= ingestion_timestamp + INTERVAL 5 MINUTES",
        "version": "payload.metadata.schema_version IN ('1.0','2.0','3.0')",
        "customer_id": "customer.customer_id IS NOT NULL AND trim(customer.customer_id) <> ''",
        "email": "operation = 'DELETE' OR customer.email RLIKE '^[^ @]+@[^ @]+[.][^ @]+$'",
        "country": "operation = 'DELETE' OR customer.addresses IS NULL OR "
                   "forall(customer.addresses, a -> coalesce(a.country IN ('India','US','GB','IN'), false))",
        "cdc": "payload.cdc IS NULL OR (entity IN ('customer','order') AND "
               "operation IN ('INSERT','UPDATE','DELETE') AND sequence_number >= 0)",
        "cdc_required": "NOT (event_type LIKE 'CUSTOMER_%' OR event_type LIKE 'ORDER_%') OR payload.cdc IS NOT NULL",
        "cdc_entity": "payload.cdc IS NULL OR ((entity='customer' AND event_type LIKE 'CUSTOMER_%') OR "
                      "(entity='order' AND event_type LIKE 'ORDER_%'))",
        "cdc_operation": "payload.cdc IS NULL OR (CASE WHEN event_type LIKE '%_CREATED' THEN operation='INSERT' "
                         "WHEN event_type LIKE '%_DELETED' THEN operation='DELETE' ELSE operation='UPDATE' END)",
        "after_image": "payload.cdc IS NULL OR operation='DELETE' OR "
                       "(entity='customer' AND payload.cdc.after.customer IS NOT NULL) OR "
                       "(entity='order' AND payload.cdc.after.order IS NOT NULL)",
        "order_id": "NOT (event_type LIKE 'ORDER_%' OR event_type IN ('PAYMENT_COMPLETED','SHIPMENT_CREATED')) OR "
                    "(order.order_id IS NOT NULL AND trim(order.order_id) <> '')",
        "items": "NOT (event_type LIKE 'ORDER_%') OR operation='DELETE' OR "
                 "(size(order.items)>0 AND forall(order.items, i -> coalesce("
                 "i.product_id IS NOT NULL AND try_cast(i.quantity AS INT)>0 AND "
                 "try_cast(i.quantity AS DECIMAL(18,4))=try_cast(i.quantity AS INT) AND "
                 "try_cast(i.unit_price AS DECIMAL(18,2))>=0, false)))",
        "currency": "NOT (event_type LIKE 'ORDER_%') OR operation='DELETE' OR order.currency IN ('INR','USD','GBP')",
        "order_time": "NOT (event_type LIKE 'ORDER_%') OR operation='DELETE' OR "
                      "try_cast(order.created_at AS TIMESTAMP) IS NOT NULL",
        "payment": "order.payment IS NULL OR (order.payment.payment_id IS NOT NULL AND "
                   "try_cast(order.payment.amount AS DECIMAL(18,2))>=0)",
        "payment_required": "event_type <> 'PAYMENT_COMPLETED' OR order.payment IS NOT NULL",
        "shipment_required": "event_type <> 'SHIPMENT_CREATED' OR order.shipping.tracking_id IS NOT NULL",
    }


def validate(parsed, lateness_seconds=1800):
    failures = [F.when(~F.coalesce(F.expr(rule), F.lit(False)), F.lit(name))
                for name, rule in validation_rules().items()]
    return (parsed.withColumn("rejection_reasons", F.filter(F.array(*failures), lambda x: x.isNotNull()))
        .withColumn("is_valid", F.size("rejection_reasons") == 0)
        .withColumn("validation_status", F.when(F.col("is_valid"), "VALID").otherwise("INVALID"))
        .withColumn("arrival_delay_seconds", F.col("ingestion_timestamp").cast("long") - F.col("event_timestamp").cast("long"))
        .withColumn("is_late_by_sla", F.col("arrival_delay_seconds") > lateness_seconds))


def deduplicate(df, watermark="30 minutes"):
    return df.withWatermark("event_timestamp", watermark).dropDuplicatesWithinWatermark(["event_id"])


def customer_changes(df):
    # HOME address chosen explicitly; an empty array produces null, not an ANSI bounds failure.
    home = F.expr("try_element_at(filter(customer.addresses, a -> a.address_type='HOME'), 1)")
    return (df.filter("entity='customer'").withColumn("home", home).select(
        "event_id", "operation", "sequence_number", "event_timestamp", "payload_hash",
        F.col("customer.customer_id").alias("customer_id"),
        F.concat_ws(" ", "customer.name.first_name", "customer.name.last_name").alias("customer_name"),
        "customer.email", "home.city", "home.state", "customer.addresses",
        "customer.loyalty_tier", "customer.marketing_preferences"))


def order_changes(df):
    return (df.filter("entity='order'").select(
        "event_id", "operation", "sequence_number", "event_timestamp", "payload_hash",
        F.col("customer.customer_id").alias("customer_id"),
        "order.*", F.col("payload.source.region").alias("region"))
        .withColumn("created_at", F.expr("try_cast(created_at as timestamp)"))
        .withColumn("order_amount", F.expr("aggregate(items, cast(0 as decimal(28,2)), "
            "(s,i) -> cast(s + try_cast(i.quantity as int)*try_cast(i.unit_price as decimal(18,2)) as decimal(28,2)))")))


def flatten_items(orders):
    # Separate item and address projections prevent a Cartesian expansion.
    return (orders.select("order_id", "customer_id", "currency", "region", "created_at", "event_timestamp",
                         F.posexplode("items").alias("item_index", "item"))
        .select("order_id", "customer_id", "currency", "region", "created_at", "event_timestamp", "item_index",
                "item.product_id", "item.product_name", "item.category",
                F.expr("try_cast(item.quantity as int)").alias("quantity"),
                F.expr("try_cast(item.unit_price as decimal(18,2))").alias("unit_price"))
        .withColumn("line_revenue", F.col("quantity") * F.col("unit_price")))


def sales_summary(items):
    return (items.groupBy(F.to_date("created_at").alias("date"), F.hour("created_at").alias("hour"),
                          "region", "category", "currency")
        .agg(F.countDistinct("order_id").alias("order_count"), F.count("item_index").alias("item_count"),
             F.sum("quantity").alias("total_quantity"), F.sum("line_revenue").alias("gross_revenue"))
        .withColumn("average_order_value", F.col("gross_revenue") / F.col("order_count")))


def window_metrics(orders, watermark="30 minutes", kind="tumbling"):
    orders = orders.withWatermark("event_timestamp", watermark)
    if kind == "session":
        groups = [F.session_window("event_timestamp", "10 minutes").alias("window"), "customer_id", "currency"]
    else:
        window = F.window("event_timestamp", "10 minutes") if kind == "tumbling" else F.window("event_timestamp", "30 minutes", "5 minutes")
        groups = [window.alias("window"), "region", "currency"]
    return (orders.groupBy(*groups).agg(F.count("order_id").alias("order_count"),
            F.sum("order_amount").alias("revenue"),
            F.sum(F.expr("aggregate(items, 0L, (s,i) -> s + try_cast(i.quantity as bigint))")).alias("quantity"))
        .withColumn("average_order_value", F.col("revenue") / F.col("order_count")))
