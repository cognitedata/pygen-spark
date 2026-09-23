"""Additional unit tests for filter helpers (non-BDD)."""

from __future__ import annotations

import pytest

from cognite.pygen_spark.filters import (
    AggregateMetric,
    AggregateRequestSpec,
    build_aggregate_payload,
    build_filter_json,
    effective_list_request_limit,
    filter_spec_from_udtf_params,
)


def test_filter_spec_from_udtf_params_roundtrip() -> None:
    spec = filter_spec_from_udtf_params(
        view_space="sp-lims",
        view_external_id="LimsResults",
        view_version="v1",
        instance_type="node",
        property_is_array={"tags": True},
        property_filters={"TestSeqNumber": "5889450"},
        exists='["DilutionFactor"]',
        instance_space="sp-lims-instances",
        gt='{"boat_guid": 1}',
    )
    result = build_filter_json(spec)
    assert result is not None
    assert "and" in result
    assert len(result["and"]) == 4


def test_aggregate_rejects_more_than_five() -> None:
    with pytest.raises(ValueError, match="at most 5"):
        build_aggregate_payload(
            AggregateRequestSpec(
                view_space="s",
                view_external_id="V",
                view_version="v1",
                aggregates=[AggregateMetric(fn="count", property="externalId")] * 6,
            )
        )


def test_effective_limit_none_row_limit() -> None:
    assert effective_list_request_limit(100, None, 0) == 100


def test_empty_filter_spec() -> None:
    from cognite.pygen_spark.filters import FilterSpec

    assert build_filter_json(FilterSpec(view_space="s", view_external_id="V", view_version="v1")) is None


def test_parse_metric_aggregates_path_list() -> None:
    from cognite.pygen_spark.filters import parse_metric_aggregates

    data = {
        "items": [
            {
                "aggregates": [
                    {
                        "aggregate": "min",
                        "property": ["sp-lims", "LimsResults/v1", "DateAuthorised"],
                        "value": 1,
                    }
                ]
            }
        ]
    }
    assert parse_metric_aggregates(data)[("min", "DateAuthorised")] == 1
