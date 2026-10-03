"""Live checks for the 100,000-node LIMS result seed on cognite-cogsail.

MIN / MAX on a text property is rejected in the generated UDTF before the CDF request
(see ``test_min_max_on_non_numeric_property_fails_with_guidance``). These tests only
send aggregates CDF can execute.
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
    effective_list_request_limit,
    parse_aggregate_count,
    parse_grouped_counts,
    parse_metric_aggregates,
)
from tests.test_live.lims_result import (
    FEBRUARY_START,
    FIXTURE_EXTERNAL_ID,
    FIXTURE_NUMERIC_VALUE,
    INSTANCE_COUNT,
    JANUARY_START,
    LAB_A_COUNT,
    LAB_B_COUNT,
    LONG_COMMENT_LENGTH,
    MOISTURE_CONTENT,
    MOISTURE_CONTENT_COUNT,
    OUT_OF_SPEC_COUNT,
    OUT_OF_SPEC_NUMERIC_VALUE,
    LimsResultSeed,
    aggregate_count,
    ensure_lims_result_seed,
    january_entered_count,
    numeric_bounds,
    properties_for,
)

INDEX_TIMEOUT_SECONDS = 900


def _filter(seed: LimsResultSeed, **kwargs: Any) -> dict[str, Any]:
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


def _wait_for_count(client: CogniteClient, seed: LimsResultSeed, instance_space: str | list[str], expected: int) -> int:
    deadline = time.monotonic() + INDEX_TIMEOUT_SECONDS
    count = aggregate_count(client, seed, instance_space)
    while count != expected and time.monotonic() < deadline:
        time.sleep(5)
        count = aggregate_count(client, seed, instance_space)
    return count


@pytest.fixture(scope="module")
def seeded_lims_result(live_client: CogniteClient) -> LimsResultSeed:
    seed = ensure_lims_result_seed(live_client)
    lab_a = _wait_for_count(live_client, seed, seed.lab_a, LAB_A_COUNT)
    lab_b = _wait_for_count(live_client, seed, seed.lab_b, LAB_B_COUNT)
    assert lab_a == LAB_A_COUNT
    assert lab_b == LAB_B_COUNT
    return seed


def _post_aggregate(client: CogniteClient, seed: LimsResultSeed, spec: AggregateRequestSpec) -> dict[str, Any]:
    url = f"/api/v1/projects/{client.config.project}/models/instances/aggregate"
    return client.post(url, json=build_aggregate_payload(spec)).json()


@pytest.mark.live
def test_instance_space_counts(live_client: CogniteClient, seeded_lims_result: LimsResultSeed) -> None:
    both = aggregate_count(live_client, seeded_lims_result, [seeded_lims_result.lab_a, seeded_lims_result.lab_b])
    view_space = aggregate_count(live_client, seeded_lims_result, seeded_lims_result.view_space)
    assert both == INSTANCE_COUNT
    assert view_space == 0


@pytest.mark.live
def test_fixture_row_round_trip(live_client: CogniteClient, seeded_lims_result: LimsResultSeed) -> None:
    retrieved = live_client.data_modeling.instances.retrieve(
        nodes=(seeded_lims_result.lab_a, FIXTURE_EXTERNAL_ID),
        sources=[seeded_lims_result.view],
    )
    assert len(retrieved.nodes) == 1
    stored = retrieved.nodes[0].properties[seeded_lims_result.view]
    expected = properties_for(1).model_dump()
    assert stored["componentName"] == MOISTURE_CONTENT
    assert stored["numericValue"] == FIXTURE_NUMERIC_VALUE
    assert stored["resultType"] == expected["resultType"]
    assert stored["statusCode"] == "AUTHORIZED"
    assert stored["isOutOfSpec"] is False
    assert stored["rowVersionChecksum"] == expected["rowVersionChecksum"]


@pytest.mark.live
def test_equality_exists_and_boolean_counts(live_client: CogniteClient, seeded_lims_result: LimsResultSeed) -> None:
    moisture = _filter(seeded_lims_result, property_filters={"componentName": MOISTURE_CONTENT})
    out_of_spec = _filter(seeded_lims_result, property_filters={"isOutOfSpec": True})
    missing_aliquot = _filter(seeded_lims_result, not_exists_properties=["aliquotId"])
    present_aliquot = _filter(seeded_lims_result, exists_properties=["aliquotId"])

    def count(filter_json: dict[str, Any]) -> int:
        payload = _post_aggregate(
            live_client,
            seeded_lims_result,
            AggregateRequestSpec(
                view_space=seeded_lims_result.view_space,
                view_external_id=seeded_lims_result.view_external_id,
                view_version=seeded_lims_result.view_version,
                aggregates=[AggregateMetric(fn="count", property="externalId")],
                filter_json=filter_json,
            ),
        )
        return parse_aggregate_count(payload)

    assert count(moisture) == MOISTURE_CONTENT_COUNT
    assert count(out_of_spec) == OUT_OF_SPEC_COUNT
    assert count(missing_aliquot) == 20_000
    assert count(present_aliquot) == INSTANCE_COUNT - 20_000


@pytest.mark.live
def test_numeric_min_max_and_january_range(live_client: CogniteClient, seeded_lims_result: LimsResultSeed) -> None:
    low, high = numeric_bounds(seeded_lims_result.lab_a)
    metrics = parse_metric_aggregates(
        _post_aggregate(
            live_client,
            seeded_lims_result,
            AggregateRequestSpec(
                view_space=seeded_lims_result.view_space,
                view_external_id=seeded_lims_result.view_external_id,
                view_version=seeded_lims_result.view_version,
                aggregates=[
                    AggregateMetric(fn="min", property="numericValue"),
                    AggregateMetric(fn="max", property="numericValue"),
                ],
                filter_json=_filter(seeded_lims_result, instance_space=seeded_lims_result.lab_a),
            ),
        )
    )
    assert metrics[("min", "numericValue")] == low
    assert metrics[("max", "numericValue")] == high
    assert high == OUT_OF_SPEC_NUMERIC_VALUE

    january = _filter(
        seeded_lims_result,
        gte={"enteredTimestamp": JANUARY_START},
        lt={"enteredTimestamp": FEBRUARY_START},
    )
    payload = _post_aggregate(
        live_client,
        seeded_lims_result,
        AggregateRequestSpec(
            view_space=seeded_lims_result.view_space,
            view_external_id=seeded_lims_result.view_external_id,
            view_version=seeded_lims_result.view_version,
            aggregates=[AggregateMetric(fn="count", property="externalId")],
            filter_json=january,
        ),
    )
    assert parse_aggregate_count(payload) == january_entered_count()


def _counts_by_group(payload: dict[str, Any], *keys: str) -> dict[tuple[object, ...], int]:
    return {tuple(group.get(key) for key in keys): count for group, count in parse_grouped_counts(payload)}


@pytest.mark.live
def test_group_by_with_where_filters(live_client: CogniteClient, seeded_lims_result: LimsResultSeed) -> None:
    seed = seeded_lims_result

    def grouped(*keys: str, group_by: list[str], **filter_kwargs: Any) -> dict[tuple[object, ...], int]:
        payload = _post_aggregate(
            live_client,
            seed,
            AggregateRequestSpec(
                view_space=seed.view_space,
                view_external_id=seed.view_external_id,
                view_version=seed.view_version,
                aggregates=[AggregateMetric(fn="count", property="externalId")],
                filter_json=_filter(seed, **filter_kwargs),
                group_by=group_by,
            ),
        )
        return _counts_by_group(payload, *keys)

    lab_a_components = grouped("componentName", group_by=["componentName"], instance_space=seed.lab_a)
    assert len(lab_a_components) == 10
    assert set(lab_a_components.values()) == {LAB_A_COUNT // 10}
    assert lab_a_components[(MOISTURE_CONTENT,)] == LAB_A_COUNT // 10

    moisture = grouped(
        "componentName",
        group_by=["componentName"],
        instance_space=seed.lab_a,
        property_filters={"componentName": MOISTURE_CONTENT},
    )
    assert moisture == {(MOISTURE_CONTENT,): LAB_A_COUNT // 10}

    by_space_and_component = grouped(
        "space",
        "componentName",
        group_by=["space", "componentName"],
        instance_space=[seed.lab_a, seed.lab_b],
    )
    lab_a_counts = {count for (space, _), count in by_space_and_component.items() if space == seed.lab_a}
    lab_b_counts = {count for (space, _), count in by_space_and_component.items() if space == seed.lab_b}
    assert len(by_space_and_component) == 20
    assert lab_a_counts == {LAB_A_COUNT // 10}
    assert lab_b_counts == {LAB_B_COUNT // 10}

    by_space = grouped("space", group_by=["space"], instance_space=[seed.lab_a, seed.lab_b])
    assert by_space == {(seed.lab_a,): LAB_A_COUNT, (seed.lab_b,): LAB_B_COUNT}


@pytest.mark.live
def test_row_limit_requests_only_that_many_rows(live_client: CogniteClient, seeded_lims_result: LimsResultSeed) -> None:
    limit = effective_list_request_limit(page_limit=1000, row_limit=2, rows_yielded=0)
    assert limit == 2
    payload = {
        "instanceType": "node",
        "sources": [
            {
                "source": {
                    "type": "view",
                    "space": seeded_lims_result.view_space,
                    "externalId": seeded_lims_result.view_external_id,
                    "version": seeded_lims_result.view_version,
                }
            }
        ],
        "filter": _filter(seeded_lims_result, instance_space=seeded_lims_result.lab_a),
        "limit": limit,
    }
    url = f"/api/v1/projects/{live_client.config.project}/models/instances/list"
    items = live_client.post(url, json=payload).json()["items"]
    assert len(items) == 2
    assert {item["space"] for item in items} == {seeded_lims_result.lab_a}


@pytest.mark.live
def test_long_audit_comment_round_trip(live_client: CogniteClient, seeded_lims_result: LimsResultSeed) -> None:
    retrieved = live_client.data_modeling.instances.retrieve(
        nodes=(seeded_lims_result.lab_a, "result_001000"),
        sources=[seeded_lims_result.view],
    )
    comment = retrieved.nodes[0].properties[seeded_lims_result.view]["auditComment"]
    assert isinstance(comment, str)
    assert len(comment) == LONG_COMMENT_LENGTH
