# Strings deliberately absorb JSON numeric/string drift; try_cast enforces business types later.
ADDRESS = "STRUCT<address_type:STRING,city:STRING,state:STRING,country:STRING,postal_code:STRING>"
CUSTOMER = f"""STRUCT<customer_id:STRING,name:STRUCT<first_name:STRING,last_name:STRING>,
email:STRING,phone:STRING,addresses:ARRAY<{ADDRESS}>,loyalty_tier:STRING,
marketing_preferences:STRUCT<email:BOOLEAN,sms:BOOLEAN>>"""
ITEM = "STRUCT<product_id:STRING,product_name:STRING,category:STRING,quantity:STRING,unit_price:STRING>"
ORDER = f"""STRUCT<order_id:STRING,order_status:STRING,currency:STRING,created_at:STRING,
items:ARRAY<{ITEM}>,payment:STRUCT<payment_id:STRING,payment_method:STRING,
payment_status:STRING,amount:STRING>,shipping:STRUCT<carrier:STRING,tracking_id:STRING,
estimated_delivery:STRING>>"""
SNAPSHOT = f"STRUCT<customer:{CUSTOMER},order:{ORDER}>"
EVENT = f"""STRUCT<event_id:STRING,event_type:STRING,event_timestamp:STRING,
source:STRUCT<system:STRING,region:STRING,application:STRING>,customer:{CUSTOMER},order:{ORDER},
metadata:STRUCT<schema_version:STRING,ingestion_source:STRING,correlation_id:STRING,replay_of:STRING>,
cdc:STRUCT<entity:STRING,operation:STRING,sequence_number:STRING,before:{SNAPSHOT},after:{SNAPSHOT}>,
_corrupt_record:STRING>"""
EVENT_TYPES = (
    "ORDER_CREATED", "ORDER_UPDATED", "ORDER_CANCELLED", "ORDER_DELETED",
    "CUSTOMER_CREATED", "CUSTOMER_UPDATED", "CUSTOMER_DELETED",
    "PAYMENT_COMPLETED", "SHIPMENT_CREATED", "PRODUCT_UPDATED",
)
