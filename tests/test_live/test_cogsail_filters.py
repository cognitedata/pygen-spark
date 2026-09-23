"""Live CDF validation of FilterDefinition JSON against cognite-cogsail.

Credentials are loaded from ``CDF_CREDENTIALS_TOML`` (default path from the plan).
Secrets are never logged. Tests skip when the TOML file is missing.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

try:
    import tomllib
except ModuleNotFoundError:  # Python < 3.11
    import tomli as tomllib  # type: ignore[import-not-found,no-redef]
from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials

from cognite.pygen_spark.filters import (
    AggregateMetric,
    AggregateRequestSpec,
    FilterSpec,
    build_aggregate_payload,
    build_filter_json,
)

DEFAULT_TOML = Path(r"C:\Users\FredrikHolm\Downloads\PRD-Data Quality\cog-sail-client_id.toml")


def _credentials_toml_path() -> Path:
    override = os.environ.get("CDF_CREDENTIALS_TOML")
    return Path(override) if override else DEFAULT_TOML


def _load_toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as fh:
        return tomllib.load(fh)


@pytest.fixture(scope="module")
def live_client() -> CogniteClient:
    path = _credentials_toml_path()
    if not path.is_file():
        pytest.skip(f"Live credentials TOML not found: {path}")

    data = _load_toml(path)
    cognite = data["cognite"]
    runtime = data.get("fn_btp_hierarchy_runtime") or {}
    client_id = runtime.get("client_id") or data.get("hierarchy_workflow_triggers", {}).get("ingest_trigger_client_id")
    client_secret = runtime.get("client_secret") or data.get("hierarchy_workflow_trigger_secrets", {}).get(
        "ingest_trigger_client_secret"
    )
    if not client_id or not client_secret:
        pytest.skip("TOML missing client_id / client_secret")

    project = cognite["project"]
    cluster = cognite["cdf_cluster"]
    tenant = cognite["idp_tenant_id"]
    base_url = f"https://{cluster}.cognitedata.com"
    creds = OAuthClientCredentials(
        token_url=f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=[f"{base_url}/.default"],
    )
    config = ClientConfig(
        client_name="pygen-spark-live-filters",
        project=project,
        credentials=creds,
        base_url=base_url,
    )
    return CogniteClient(config)


@pytest.fixture(scope="module")
def sample_view_ids(live_client: CogniteClient) -> tuple[str, str, str]:
    """Return (space, external_id, version) for a readable node view with instances."""
    views = live_client.data_modeling.views.list(limit=50)
    for view in views:
        if getattr(view, "used_for", None) == "edge":
            continue
        try:
            items = live_client.data_modeling.instances.list(
                sources=view.as_id(),
                instance_type="node",
                limit=1,
            )
        except Exception:
            continue
        if items:
            return view.space, view.external_id, view.version
    pytest.skip("No readable node view with instances found in cognite-cogsail")


@pytest.mark.live
def test_live_equals_filter_matches_sdk(
    live_client: CogniteClient,
    sample_view_ids: tuple[str, str, str],
) -> None:
    space, external_id, version = sample_view_ids
    view_id = live_client.data_modeling.views.retrieve((space, external_id, version))[0].as_id()
    sample = live_client.data_modeling.instances.list(sources=view_id, instance_type="node", limit=1)[0]
    props = sample.properties.get(view_id) if hasattr(sample, "properties") else None
    if not props:
        # Fallback: dump properties dict
        prop_map = getattr(sample, "properties", {}) or {}
        # properties keyed by ViewId
        for _key, value in prop_map.items():
            if value:
                props = value
                break
    if not props:
        pytest.skip("Sample instance has no view properties to filter on")

    prop_name, prop_value = next(iter(props.items()))
    if prop_value is None or isinstance(prop_value, (list, dict)):
        pytest.skip("Need a scalar property value for equals filter")

    filter_json = build_filter_json(
        FilterSpec(
            view_space=space,
            view_external_id=external_id,
            view_version=version,
            property_filters={prop_name: prop_value},
        )
    )
    assert filter_json is not None

    # REST via SDK advanced filter: use instances.list with filter dict through cognite client
    from cognite.client.data_classes.filters import Equals

    sdk_filter = Equals(property=[space, f"{external_id}/{version}", prop_name], value=prop_value)
    sdk_items = live_client.data_modeling.instances.list(
        sources=view_id,
        instance_type="node",
        filter=sdk_filter,
        limit=100,
    )
    assert len(sdk_items) >= 1

    # Validate our JSON shape matches Equals serialization
    assert filter_json == {
        "equals": {
            "property": [space, f"{external_id}/{version}", prop_name],
            "value": prop_value,
        }
    }


@pytest.mark.live
def test_live_instance_space_filter(
    live_client: CogniteClient,
    sample_view_ids: tuple[str, str, str],
) -> None:
    space, external_id, version = sample_view_ids
    view_id = live_client.data_modeling.views.retrieve((space, external_id, version))[0].as_id()
    sample = live_client.data_modeling.instances.list(sources=view_id, instance_type="node", limit=1)[0]
    instance_space = sample.space

    filter_json = build_filter_json(
        FilterSpec(
            view_space=space,
            view_external_id=external_id,
            view_version=version,
            instance_space=instance_space,
        )
    )
    assert filter_json == {
        "equals": {
            "property": ["node", "space"],
            "value": instance_space,
        }
    }

    from cognite.client.data_classes.filters import Equals

    sdk_items = live_client.data_modeling.instances.list(
        sources=view_id,
        instance_type="node",
        filter=Equals(property=["node", "space"], value=instance_space),
        limit=5,
    )
    assert len(sdk_items) >= 1
    assert all(item.space == instance_space for item in sdk_items)


@pytest.mark.live
def test_live_count_aggregate_matches_tight_list(
    live_client: CogniteClient,
    sample_view_ids: tuple[str, str, str],
) -> None:
    space, external_id, version = sample_view_ids
    view_id = live_client.data_modeling.views.retrieve((space, external_id, version))[0].as_id()
    sample = live_client.data_modeling.instances.list(sources=view_id, instance_type="node", limit=1)[0]
    ext_id = sample.external_id

    filter_json = build_filter_json(
        FilterSpec(
            view_space=space,
            view_external_id=external_id,
            view_version=version,
            external_id=ext_id,
            instance_space=sample.space,
        )
    )
    payload = build_aggregate_payload(
        AggregateRequestSpec(
            view_space=space,
            view_external_id=external_id,
            view_version=version,
            aggregates=[AggregateMetric(fn="count", property="externalId")],
            filter_json=filter_json,
        )
    )
    assert payload["aggregates"] == [{"count": {"property": "externalId"}}]
    assert "sources" not in payload
    assert payload["filter"] == filter_json

    from cognite.client.data_classes.aggregations import Count
    from cognite.client.data_classes.filters import And, Equals

    sdk_filter = And(
        Equals(property=["node", "space"], value=sample.space),
        Equals(property=["node", "externalId"], value=ext_id),
    )
    result = live_client.data_modeling.instances.aggregate(
        view=view_id,
        aggregates=Count("externalId"),
        filter=sdk_filter,
    )
    # SDK returns AggregationResult list / object depending on version
    values = getattr(result, "value", None)
    if values is None and hasattr(result, "__iter__"):
        items = list(result)
        assert len(items) >= 1
        count_val = getattr(items[0], "value", items[0])
        assert int(count_val) == 1
    else:
        assert values is not None
        assert int(values) == 1
