# Databricks notebook source
# Execute after an isolated test-catalog run with sample_data/scenarios.json only.
import re

dbutils.widgets.text("catalog", "retail_test")
c = dbutils.widgets.get("catalog")
assert re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", c)
def scalar(query):
    return spark.sql(query).first()[0]
assert scalar(f"SELECT count(*) FROM {c}.bronze.raw_events") == 18
assert scalar(f"SELECT count(*) FROM {c}.dq.invalid_events") == 5
assert scalar(f"SELECT count(*) FROM {c}.silver.customers WHERE customer_id='C001'") == 0
assert scalar(f"SELECT count(*) FROM {c}.silver.customer_history WHERE customer_id='C001'") == 3
assert scalar(f"SELECT count(*) FROM {c}.silver.orders WHERE order_id='ORD000001' AND order_status='CANCELLED'") == 1
assert scalar(f"SELECT count(*) FROM {c}.silver.order_items WHERE order_id='ORD000001'") == 0
assert scalar(f"SELECT count(*) FROM {c}.silver.orders WHERE order_id='ORD000009'") == 1
assert scalar(f"SELECT count(*) FROM {c}.silver.orders WHERE order_id='ORD000006'") == 1
assert scalar(f"SELECT count(*) FROM {c}.gold.sales_summary WHERE gross_revenue < 0") == 0
# Restart pipeline without changing files; re-run to verify Bronze count remains 18.
print("Databricks acceptance assertions passed")
