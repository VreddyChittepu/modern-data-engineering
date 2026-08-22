from pathlib import Path

from streaming_api_lakehouse.api_catalog import STREAMING_MODES, ApiCatalog


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_catalog_contains_only_streaming_modes():
    catalog = ApiCatalog.load(PROJECT_ROOT / "configs" / "streaming_apis.json")

    assert catalog.raw_topic == "api.raw.events"
    assert catalog.apis
    assert {api.ingestion_mode for api in catalog.apis} <= STREAMING_MODES


def test_catalog_includes_wikimedia_native_stream():
    catalog = ApiCatalog.load(PROJECT_ROOT / "configs" / "streaming_apis.json")
    api = catalog.get("wikimedia_eventstreams")

    assert api.ingestion_mode == "server_sent_events"
    assert "stream.wikimedia.org" in api.url
