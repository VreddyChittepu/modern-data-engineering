"""HTTP helpers for streaming-style API producers."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

import requests


def get_json_with_retry(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    attempts: int = 3,
    backoff_seconds: float = 2.0,
    session_factory: Callable[[], requests.Session] = requests.Session,
) -> Any:
    """GET JSON with simple exponential backoff for polling producers."""

    last_error: Exception | None = None
    with session_factory() as session:
        for attempt in range(1, attempts + 1):
            try:
                response = session.get(url, params=params, headers=headers, timeout=30)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                last_error = exc
                if attempt == attempts:
                    break
                time.sleep(backoff_seconds * attempt)
    raise RuntimeError(f"Failed to fetch {url}") from last_error
