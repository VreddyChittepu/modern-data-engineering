"""Kafka producer for Wikimedia EventStreams recent-change events."""

from __future__ import annotations

import argparse
import json

import requests
from confluent_kafka import Producer
from sseclient import SSEClient

from streaming_api_lakehouse.api_catalog import ApiCatalog
from streaming_api_lakehouse.producers.base import build_envelope, publish_json


def stream_recent_changes(catalog_path: str, bootstrap_servers: str, topic: str | None = None) -> None:
    catalog = ApiCatalog.load(catalog_path)
    api = catalog.get("wikimedia_eventstreams")
    target_topic = topic or catalog.raw_topic
    producer = Producer({"bootstrap.servers": bootstrap_servers})

    response = requests.get(api.url, stream=True, timeout=60)
    response.raise_for_status()

    for event in SSEClient(response).events():
        if not event.data or event.data == "[DONE]":
            continue
        payload = json.loads(event.data)
        key = str(payload.get("id") or payload.get("meta", {}).get("id") or payload.get("wiki", api.name))
        envelope = build_envelope(api, payload, {"stream": "recentchange"})
        publish_json(producer, target_topic, key, envelope)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    stream_recent_changes(args.catalog, args.bootstrap_servers, args.topic)
