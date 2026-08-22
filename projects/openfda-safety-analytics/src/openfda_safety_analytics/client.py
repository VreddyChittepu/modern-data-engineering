"""Small openFDA REST client for incremental ingestion."""

from __future__ import annotations

import os
import time
from typing import Any

import requests

from openfda_safety_analytics.catalog import OpenFdaCatalog, OpenFdaEndpoint


class OpenFdaClient:
    def __init__(self, catalog: OpenFdaCatalog, api_key: str | None = None) -> None:
        self.catalog = catalog
        self.api_key = api_key or os.getenv("OPENFDA_API_KEY")

    def fetch(
        self,
        endpoint: OpenFdaEndpoint,
        *,
        search: str | None = None,
        limit: int | None = None,
        skip: int = 0,
        count: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "limit": limit or self.catalog.default_limit,
            "skip": skip,
        }
        if search:
            params["search"] = search
        if count:
            params["count"] = count
            params.pop("skip", None)
        if self.api_key:
            params["api_key"] = self.api_key

        response = requests.get(self.catalog.url_for(endpoint), params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def fetch_with_retry(
        self,
        endpoint: OpenFdaEndpoint,
        *,
        search: str | None = None,
        limit: int | None = None,
        skip: int = 0,
        attempts: int = 3,
    ) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                return self.fetch(endpoint, search=search, limit=limit, skip=skip)
            except requests.RequestException as exc:
                last_error = exc
                if attempt == attempts:
                    break
                time.sleep(2 * attempt)
        raise RuntimeError(f"openFDA request failed for {endpoint.name}") from last_error
