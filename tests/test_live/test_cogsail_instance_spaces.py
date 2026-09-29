"""Live pushdown against dedicated instance spaces (instance space differs from view space ``sailboat``).

Posts the exact filter / aggregate JSON the generated UDTFs send, so these tests catch
view-space vs instance-space confusion that the ``sailboat``-only data cannot.
"""

from __future__ import annotations

import time
from typing import Any

import pytest
from cognite.client import CogniteClient

from cognite.pygen_spark.filters import (
    AggregateMetric,
    AggregateRequestSpec,
    FilterSpec,
    build_aggregate_payload,
    build_filter_json,
    parse_aggregate_count,
    parse_metric_aggregates,
)
from tests.test_live.cogsail import CogsailSeed

FLEET_A = "inst_sailboat_fleet_a"
FLEET_B = "inst_sailboat_fleet_b"


def _filter(seed: CogsailSeed, **kwargs: Any) -> dict[str, Any]:
    filter_json = build_filter_json(
        FilterSpec(
            view_space=seed.view_space,
            view_external_id=seed.view_external_id,
            view_version=seed.view_version,
            **kwargs,
        )
    )
    assert filter_json is not None
    return filter_json


def _list_external_ids(client: CogniteClient, seed: CogsailSeed, filter_json: dict[str, Any]) -> set[str]:
    payload = {
        "instanceType": "node",
        "sources": [
            {
                "source": {
                    "type": "view",
                    "space": seed.view_space,
                    "externalId": seed.view_external_id,
                    "version": seed.view_version,
                }
            }
        ],
        "filter": filter_json,
        "limit": 1000,
    }
    response = client.post(f"/api/v1/projects/{client.config.project}/models/instances/list", json=payload)
    return {item["externalId"] for item in response.json()["items"]}


@pytest.mark.live
def test_instance_space_equals_returns_only_that_space(live_client: CogniteClient, seeded_cogsail: CogsailSeed) -> None:
    found = _list_external_ids(live_client, seeded_cogsail, _filter(seeded_cogsail, instance_space=FLEET_A))
    assert found == seeded_cogsail.by_space[FLEET_A].external_ids


@pytest.mark.live
def test_instance_space_in_list_returns_both_spaces(live_client: CogniteClient, seeded_cogsail: CogsailSeed) -> None:
    found = _list_external_ids(live_client, seeded_cogsail, _filter(seeded_cogsail, instance_space=[FLEET_A, FLEET_B]))
    expected = seeded_cogsail.by_space[FLEET_A].external_ids | seeded_cogsail.by_space[FLEET_B].external_ids
    assert found == expected


@pytest.mark.live
def test_view_space_is_not_an_instance_space_filter(live_client: CogniteClient, seeded_cogsail: CogsailSeed) -> None:
    """Filtering on the view space must not return nodes stored in the seed instance spaces."""
    found = _list_external_ids(
        live_client, seeded_cogsail, _filter(seeded_cogsail, instance_space=seeded_cogsail.view_space)
    )
    seeded = seeded_cogsail.by_space[FLEET_A].external_ids | seeded_cogsail.by_space[FLEET_B].external_ids
    assert found.isdisjoint(seeded)


@pytest.mark.live
def test_exists_combined_with_instance_space(live_client: CogniteClient, seeded_cogsail: CogsailSeed) -> None:
    found = _list_external_ids(
        live_client,
        seeded_cogsail,
        _filter(seeded_cogsail, instance_space=FLEET_A, exists_properties=["description"]),
    )
    assert found == seeded_cogsail.by_space[FLEET_A].described_external_ids


@pytest.mark.live
def test_count_aggregate_scoped_to_instance_space(live_client: CogniteClient, seeded_cogsail: CogsailSeed) -> None:
    payload = build_aggregate_payload(
        AggregateRequestSpec(
            view_space=seeded_cogsail.view_space,
            view_external_id=seeded_cogsail.view_external_id,
            view_version=seeded_cogsail.view_version,
            aggregates=[AggregateMetric(fn="count", property="externalId")],
            filter_json=_filter(seeded_cogsail, instance_space=FLEET_B),
        )
    )
    expected = len(seeded_cogsail.by_space[FLEET_B].boats)
    url = f"/api/v1/projects/{live_client.config.project}/models/instances/aggregate"
    # Aggregates are served from an eventually consistent index; a fresh seed can take a few seconds.
    deadline = time.monotonic() + 30
    count = parse_aggregate_count(live_client.post(url, json=payload).json())
    while count != expected and time.monotonic() < deadline:
        time.sleep(3)
        count = parse_aggregate_count(live_client.post(url, json=payload).json())
    assert count == expected


@pytest.mark.live
def test_min_max_numeric_aggregate_scoped_to_instance_space(
    live_client: CogniteClient, seeded_cogsail: CogsailSeed
) -> None:
    certificates = seeded_cogsail.by_space[FLEET_A].certificates
    payload = build_aggregate_payload(
        AggregateRequestSpec(
            view_space=seeded_cogsail.view_space,
            view_external_id=seeded_cogsail.certificate_view_external_id,
            view_version=seeded_cogsail.view_version,
            aggregates=[AggregateMetric(fn="min", property="aph_tod"), AggregateMetric(fn="max", property="aph_tod")],
            filter_json=build_filter_json(
                FilterSpec(
                    view_space=seeded_cogsail.view_space,
                    view_external_id=seeded_cogsail.certificate_view_external_id,
                    view_version=seeded_cogsail.view_version,
                    instance_space=FLEET_A,
                )
            ),
        )
    )
    url = f"/api/v1/projects/{live_client.config.project}/models/instances/aggregate"
    expected = {
        ("min", "aph_tod"): min(c.aph_tod for c in certificates),
        ("max", "aph_tod"): max(c.aph_tod for c in certificates),
    }
    deadline = time.monotonic() + 30
    metrics = parse_metric_aggregates(live_client.post(url, json=payload).json())
    while metrics != expected and time.monotonic() < deadline:
        time.sleep(3)
        metrics = parse_metric_aggregates(live_client.post(url, json=payload).json())
    assert metrics == expected
