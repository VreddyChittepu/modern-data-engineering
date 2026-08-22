"""Generic polling producer for streaming-style APIs from the catalog."""

from __future__ import annotations

import argparse
import time
from collections.abc import Iterable
from typing import Any

from confluent_kafka import Producer

from streaming_api_lakehouse.api_catalog import ApiCatalog
from streaming_api_lakehouse.producers.base import build_envelope, publish_json
from streaming_api_lakehouse.utils.http import get_json_with_retry


def iter_records(payload: Any) -> Iterable[Any]:
    if isinstance(payload, list):
        yield from payload
    elif isinstance(payload, dict):
        for field in ("features", "events", "items", "results", "data"):
            value = payload.get(field)
            if isinstance(value, list):
                yield from value
                return
        yield payload
    else:
        yield payload


def poll_api(
    catalog_path: str,
    api_name: str,
    bootstrap_servers: str,
    interval_seconds: int,
    topic: str | None = None,
) -> None:
    catalog = ApiCatalog.load(catalog_path)
    api = catalog.get(api_name)
    target_topic = topic or catalog.raw_topic
    producer = Producer({"bootstrap.servers": bootstrap_servers})

    while True:
        payload = get_json_with_retry(api.url)
        for record in iter_records(payload):
            key = f"{api.name}:{hash(str(record))}"
            envelope = build_envelope(api, record, {"ingestion_mode": api.ingestion_mode})
            publish_json(producer, target_topic, key, envelope)
        producer.flush()
        time.sleep(interval_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--api-name", required=True)
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--interval-seconds", type=int, default=60)
    parser.add_argument("--topic")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    poll_api(
        args.catalog,
        args.api_name,
        args.bootstrap_servers,
        args.interval_seconds,
        args.topic,
    )
