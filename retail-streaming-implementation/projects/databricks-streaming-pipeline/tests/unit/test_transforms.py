import json
from datetime import datetime, timezone
from decimal import Decimal
import pytest
from retail.transforms import parse_events, validate, order_changes, customer_changes, flatten_items, sales_summary
from scripts.generate import event, scenarios

NOW = datetime(2026, 9, 6, 10, 0, tzinfo=timezone.utc)
def frame(spark, records):
    from pyspark.sql import functions as F
    encoded = [json.dumps(r) if isinstance(r, dict) else r for r in records]
    # JVM-native inputs keep these expression tests independent of Python worker daemons.
    return spark.range(len(encoded)).select(
        F.element_at(F.array(*[F.lit(r) for r in encoded]), (F.col("id")+1).cast("int")).alias("raw_payload"),
        F.lit(NOW).alias("ingestion_timestamp"))


def test_parse_quarantine_and_evolution(spark):
    result = validate(parse_events(frame(spark, scenarios(NOW)))).collect()
    assert len(result) == 18
    invalid = [r for r in result if not r.is_valid]
    assert len(invalid) == 5
    assert any("json" in r.rejection_reasons for r in invalid)
    evolved = next(r for r in result if r.event_id == "evt-9-ORDER_CREATED")
    assert evolved.customer.marketing_preferences.email is True
    assert evolved.customer.loyalty_tier == "GOLD"
    assert next(r for r in result if r.event_id == "evt-6-ORDER_CREATED").is_late_by_sla

@pytest.mark.parametrize("field,value,reason", [
    ("event_id", None, "event_id"), ("event_timestamp", "bad", "timestamp"),
    ("event_type", "NOPE", "event_type"),
])
def test_invalid_envelope(spark, field, value, reason):
    e = event(1, NOW); e[field] = value
    assert reason in validate(parse_events(frame(spark, [e]))).first().rejection_reasons

def test_invalid_nested_and_explicit_null(spark):
    data = []
    for quantity in ("0", "-1", "1.5", None, "oops"):
        e = event(1, NOW)
        e["cdc"]["after"]["order"]["items"][0]["quantity"] = quantity
        data.append(e)
    e = event(2, NOW); e["customer"]["email"] = "not-an-email"; data.append(e)
    e = event(3, NOW); e["customer"]["addresses"][0]["country"] = "INVALID"; data.append(e)
    rows = validate(parse_events(frame(spark, data))).collect()
    assert all(not r.is_valid for r in rows)
    assert "email" in rows[-2].rejection_reasons
    assert "country" in rows[-1].rejection_reasons

def test_items_do_not_multiply_by_addresses(spark):
    orders = order_changes(parse_events(frame(spark, [event(1, NOW)])))
    assert orders.first().order_amount == Decimal("151500.00")
    items = flatten_items(orders).collect()
    assert len(items) == 2  # two items, two addresses: not four rows
    assert sum(r.line_revenue for r in items) == Decimal("151500.00")

def test_category_distinct_order_count(spark):
    e = event(1, NOW)
    e["cdc"]["after"]["order"]["items"][1]["category"] = "Electronics"
    summary = sales_summary(flatten_items(order_changes(parse_events(frame(spark, [e]))))).first()
    assert summary.order_count == 1
    assert summary.item_count == 2
    assert summary.average_order_value == Decimal("151500.00")

def test_cdc_delete_and_out_of_order_projection(spark):
    records = [r for r in scenarios(NOW) if isinstance(r, dict) and r["event_type"].startswith("CUSTOMER")]
    rows = customer_changes(parse_events(frame(spark, records))).collect()
    latest = max(rows, key=lambda r: r.sequence_number)
    assert latest.operation == "DELETE" and latest.customer_id == "C001"
    assert next(r for r in rows if r.sequence_number == 105).city == "Hyderabad"
    assert next(r for r in rows if r.sequence_number == 102).city == "Mumbai"
    # Tests source preparation, not a substitute for Databricks AUTO CDC engine acceptance.
