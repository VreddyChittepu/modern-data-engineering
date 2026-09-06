"""NDJSON generator. Local paths or /Volumes paths on Databricks; no cloud credentials needed."""
import argparse
import copy
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


def event(number=1, now=None, event_type="ORDER_CREATED", version="1.0"):
    now = now or datetime.now(timezone.utc)
    customer = {"customer_id": f"C{number % 20:03}", "name": {"first_name": "Ravi", "last_name": "Kumar"},
        "email": f"ravi{number % 20}@example.com", "phone": None,
        "addresses": [{"address_type": "HOME", "city": "Bengaluru", "state": "Karnataka", "country": "India", "postal_code": "560001"},
                      {"address_type": "WORK", "city": "Bengaluru", "state": "Karnataka", "country": "India", "postal_code": "560002"}]}
    if version in ("2.0", "3.0"):
        customer["loyalty_tier"] = "GOLD"
    if version == "3.0":
        customer["marketing_preferences"] = {"email": True, "sms": False}
    order = {"order_id": f"ORD{number:06}", "order_status": "CONFIRMED", "currency": "INR", "created_at": now.isoformat(),
        "items": [{"product_id": "P100", "product_name": "Laptop", "category": "Electronics", "quantity": "2", "unit_price": 75000},
                  {"product_id": "P200", "product_name": "Mouse", "category": "Accessories", "quantity": 1, "unit_price": "1500.00"}],
        "payment": {"payment_id": f"PAY{number}", "payment_method": "UPI", "payment_status": "SUCCESS", "amount": 151500},
        "shipping": {"carrier": "DHL", "tracking_id": f"TRK{number}", "estimated_delivery": (now + timedelta(days=4)).date().isoformat()}}
    data = {"event_id": f"evt-{number}-{event_type}", "event_type": event_type, "event_timestamp": now.isoformat(),
        "source": {"system": "ecommerce", "region": "IN", "application": "web"}, "customer": customer, "order": order,
        "metadata": {"schema_version": version, "ingestion_source": "autoloader", "correlation_id": f"corr-{number}"}}
    if event_type.startswith(("CUSTOMER_", "ORDER_")):
        entity = "customer" if event_type.startswith("CUSTOMER_") else "order"
        operation = "INSERT" if event_type.endswith("CREATED") else "DELETE" if event_type.endswith("DELETED") else "UPDATE"
        data["cdc"] = {"entity": entity, "operation": operation, "sequence_number": number,
            "before": {entity: copy.deepcopy(data[entity])} if operation != "INSERT" else None,
            "after": {entity: copy.deepcopy(data[entity])} if operation != "DELETE" else None}
    return data


def scenarios(now=None):
    """Deterministic expected data: 151500 INR/order, customer C001 moves and is deleted."""
    now = now or datetime.now(timezone.utc)
    created = event(1, now)
    customer = event(1, now, "CUSTOMER_CREATED")
    updated = copy.deepcopy(customer)
    updated.update(event_id="customer-update-105", event_type="CUSTOMER_UPDATED", event_timestamp=(now+timedelta(seconds=1)).isoformat())
    updated["cdc"].update(operation="UPDATE", sequence_number=105, before=copy.deepcopy(customer["cdc"]["after"]))
    updated["cdc"]["after"]["customer"]["addresses"][0]["city"] = "Hyderabad"
    old = copy.deepcopy(updated)
    old["event_id"] = "customer-update-102"
    old["event_timestamp"] = (now+timedelta(milliseconds=500)).isoformat()
    old["cdc"]["sequence_number"] = 102
    old["cdc"]["after"]["customer"]["addresses"][0]["city"] = "Mumbai"
    deleted = copy.deepcopy(updated)
    deleted.update(event_id="customer-delete-110", event_type="CUSTOMER_DELETED", event_timestamp=(now+timedelta(seconds=2)).isoformat())
    deleted["cdc"].update(operation="DELETE", sequence_number=110, before=copy.deepcopy(updated["cdc"]["after"]), after=None)
    invalid = event(2, now)
    invalid["cdc"]["after"]["order"]["items"][0]["quantity"] = "bad"
    missing = event(3, now); missing.pop("event_id")
    null_email = event(4, now); null_email["customer"]["email"] = None
    bad_type = event(5, now); bad_type["event_type"] = "UNKNOWN"
    late = event(6, now-timedelta(hours=2))
    cancel = event(7, now, "ORDER_CANCELLED")
    cancel["order"]["order_id"] = "ORD000001"
    cancel["order"]["created_at"] = now.isoformat()
    cancel["order"]["order_status"] = "CANCELLED"
    cancel["cdc"]["after"]["order"] = copy.deepcopy(cancel["order"])
    return [created, copy.deepcopy(created), customer, updated, copy.deepcopy(updated), old, deleted,
        invalid, missing, null_email, bad_type, late, cancel,
        event(8, now, version="2.0"), event(9, now, version="3.0"),
        event(10, now, "PAYMENT_COMPLETED"), event(11, now, "SHIPMENT_CREATED"), '{"event_id": BROKEN']


def publish(directory, records):
    directory = Path(directory); directory.mkdir(parents=True, exist_ok=True)
    # Write complete immutable files, stage outside the watched directory, then rename.
    name = f"batch-{uuid.uuid4().hex}.json"
    staging = directory.parent / f".{name}.tmp"
    staging.write_text("\n".join(r if isinstance(r, str) else json.dumps(r) for r in records) + "\n")
    staging.replace(directory / name)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--batches", type=int, default=1, help="0 runs continuously")
    parser.add_argument("--interval", type=float, default=5)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    rng = random.Random(args.seed)
    batch = 0
    while args.batches == 0 or batch < args.batches:
        if batch == 0:
            records = scenarios()
        else:
            # Fresh monotonic sequence and unique order keys on every batch.
            base = time.time_ns()
            records = [event(base+i, event_type=rng.choice(["ORDER_CREATED", "CUSTOMER_CREATED", "CUSTOMER_UPDATED", "CUSTOMER_DELETED", "ORDER_UPDATED", "ORDER_CANCELLED", "PAYMENT_COMPLETED", "SHIPMENT_CREATED"]), version=rng.choice(["1.0","2.0","3.0"])) for i in range(10)]
            records.append(copy.deepcopy(records[0]))
            records.append(event(base+11, datetime.now(timezone.utc)-timedelta(hours=2)))
            records.append('{"broken":')
        publish(args.output, records)
        batch += 1
        if args.batches == 0 or batch < args.batches:
            time.sleep(args.interval)
if __name__ == "__main__":
    main()
