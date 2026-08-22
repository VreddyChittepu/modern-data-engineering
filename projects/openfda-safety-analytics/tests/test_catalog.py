from pathlib import Path

from openfda_safety_analytics.catalog import OpenFdaCatalog


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_catalog_loads_openfda_adverse_event_endpoints():
    catalog = OpenFdaCatalog.load(PROJECT_ROOT / "configs" / "openfda_endpoints.json")

    assert catalog.base_url == "https://api.fda.gov"
    assert catalog.default_limit == 100
    assert {endpoint.name for endpoint in catalog.endpoints} == {
        "drug_adverse_events",
        "device_adverse_events",
        "food_adverse_events",
    }


def test_catalog_builds_drug_event_url():
    catalog = OpenFdaCatalog.load(PROJECT_ROOT / "configs" / "openfda_endpoints.json")
    endpoint = catalog.get("drug_adverse_events")

    assert catalog.url_for(endpoint) == "https://api.fda.gov/drug/event.json"
