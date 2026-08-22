"""Load and validate the streaming API catalog."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


STREAMING_MODES = {
    "server_sent_events",
    "polling_or_gtfs_realtime",
    "polling_or_market_feed",
    "polling",
    "incremental_polling",
    "sensor_polling",
    "continuous_polling",
}


@dataclass(frozen=True)
class ApiDefinition:
    name: str
    display_name: str
    domain: str
    access: str
    url: str
    ingestion_mode: str
    event_type: str
    project_idea: str

    @classmethod
    def from_dict(cls, payload: dict) -> "ApiDefinition":
        missing = {
            "name",
            "display_name",
            "domain",
            "access",
            "url",
            "ingestion_mode",
            "event_type",
            "project_idea",
        } - payload.keys()
        if missing:
            raise ValueError(f"API definition is missing fields: {sorted(missing)}")
        if payload["ingestion_mode"] not in STREAMING_MODES:
            raise ValueError(f"Unsupported streaming ingestion mode: {payload['ingestion_mode']}")
        return cls(**payload)


@dataclass(frozen=True)
class ApiCatalog:
    raw_topic: str
    apis: tuple[ApiDefinition, ...]

    @classmethod
    def load(cls, path: str | Path) -> "ApiCatalog":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        apis = tuple(ApiDefinition.from_dict(item) for item in data["apis"])
        return cls(raw_topic=data["raw_topic"], apis=apis)

    def get(self, name: str) -> ApiDefinition:
        for api in self.apis:
            if api.name == name:
                return api
        raise KeyError(f"Unknown API: {name}")

    def names(self) -> Iterable[str]:
        return (api.name for api in self.apis)
