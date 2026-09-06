# Databricks notebook source
# Run as a provisioning principal; grant runtime principal access afterward (sql/security.sql).
import re

dbutils.widgets.text("catalog", "retail_dev")
catalog = dbutils.widgets.get("catalog")
assert re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog)
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
for schema in ("bronze", "silver", "gold", "dq"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
for volume in ("landing", "pipeline_state"):
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze.{volume}")
dbutils.fs.mkdirs(f"/Volumes/{catalog}/bronze/landing/orders")
