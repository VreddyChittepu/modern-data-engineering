"""Endpoint catalog for openFDA ingestion."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class OpenFdaEndpoint:
    name: str
    path: str
    domain: str
    date_field: str
    description: str


@dataclass(frozen=True)
class OpenFdaCatalog:
    base_url: str
    default_limit: int
    endpoints: tuple[OpenFdaEndpoint, ...]

    @classmethod
    def load(cls, path: str | Path) -> "OpenFdaCatalog":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        endpoints = tuple(OpenFdaEndpoint(**item) for item in payload["endpoints"])
        return cls(
            base_url=payload["base_url"].rstrip("/"),
            default_limit=int(payload["default_limit"]),
            endpoints=endpoints,
        )

    def get(self, name: str) -> OpenFdaEndpoint:
        for endpoint in self.endpoints:
            if endpoint.name == name:
                return endpoint
        raise KeyError(f"Unknown openFDA endpoint: {name}")

    def url_for(self, endpoint: OpenFdaEndpoint) -> str:
        return f"{self.base_url}{endpoint.path}"
