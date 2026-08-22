"""Shared producer utilities."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Producer

from streaming_api_lakehouse.api_catalog import ApiDefinition


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_envelope(api: ApiDefinition, payload: Any, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "source_api": api.name,
        "source_domain": api.domain,
        "event_type": api.event_type,
        "event_time": _event_time(payload),
        "ingestion_time": utc_now_iso(),
        "payload": payload,
        "metadata": metadata or {},
    }


def publish_json(producer: Producer, topic: str, key: str, value: dict[str, Any]) -> None:
    producer.produce(topic, key=key, value=json.dumps(value, separators=(",", ":")).encode("utf-8"))
    producer.poll(0)


def _event_time(payload: Any) -> str | None:
    if isinstance(payload, dict):
        for field in ("meta", "timestamp", "time", "event_time"):
            value = payload.get(field)
            if isinstance(value, dict) and "dt" in value:
                return value["dt"]
            if value is not None:
                return str(value)
    return None
