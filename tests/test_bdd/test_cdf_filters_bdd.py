"""pytest-bdd step definitions for CDF filter / LIMIT / aggregate features."""

from __future__ import annotations

import json
from typing import Any

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from cognite.pygen_spark.filters import (
    AggregateMetric,
    AggregateRequestSpec,
    FilterSpec,
    build_aggregate_payload,
    build_filter_json,
    effective_list_request_limit,
    parse_aggregate_count,
    parse_metric_aggregates,
)

scenarios("../features/cdf_list_filters.feature")
scenarios("../features/cdf_list_limit.feature")
scenarios("../features/cdf_aggregates.feature")


@pytest.fixture
def filter_ctx() -> dict[str, Any]:
    return {
        "spec": None,
        "property_is_array": {},
        "property_filters": {},
        "exists_properties": [],
        "not_exists_properties": [],
        "instance_space": None,
        "external_id": None,
        "gt": {},
        "gte": {},
        "lt": {},
        "lte": {},
        "result": None,
    }


@pytest.fixture
def limit_ctx() -> dict[str, Any]:
    return {"page_limit": 0, "row_limit": None, "rows_yielded": 0, "result": None}


@pytest.fixture
def agg_ctx() -> dict[str, Any]:
    return {
        "view_space": "",
        "view_external_id": "",
        "view_version": "",
        "instance_type": "node",
        "metrics": [],
        "filter_json": None,
        "payload": None,
        "response": None,
        "count": None,
        "metrics_parsed": None,
    }


def _rebuild_spec(ctx: dict[str, Any]) -> FilterSpec:
    base: FilterSpec = ctx["spec"]
    return FilterSpec(
        view_space=base.view_space,
        view_external_id=base.view_external_id,
        view_version=base.view_version,
        instance_type=base.instance_type,
        property_is_array=dict(ctx["property_is_array"]),
        property_filters=dict(ctx["property_filters"]),
        exists_properties=list(ctx["exists_properties"]),
        not_exists_properties=list(ctx["not_exists_properties"]),
        instance_space=ctx["instance_space"],
        external_id=ctx["external_id"],
        gt=dict(ctx["gt"]),
        gte=dict(ctx["gte"]),
        lt=dict(ctx["lt"]),
        lte=dict(ctx["lte"]),
    )


@given(
    parsers.parse(
        'a view "{external_id}" in space "{space}" version "{version}" for instance type "{instance_type}"'
    )
)
def given_view(filter_ctx: dict[str, Any], external_id: str, space: str, version: str, instance_type: str) -> None:
    filter_ctx["spec"] = FilterSpec(
        view_space=space,
        view_external_id=external_id,
        view_version=version,
        instance_type=instance_type,  # type: ignore[arg-type]
    )


@given(parsers.parse('property "{prop}" is a scalar'))
def given_scalar_prop(filter_ctx: dict[str, Any], prop: str) -> None:
    filter_ctx["property_is_array"][prop] = False


@given(parsers.parse('property "{prop}" is an array'))
def given_array_prop(filter_ctx: dict[str, Any], prop: str) -> None:
    filter_ctx["property_is_array"][prop] = True


@when(parsers.parse('I build a filter with property "{prop}" equal to "{value}"'))
def when_equals_str(filter_ctx: dict[str, Any], prop: str, value: str) -> None:
    filter_ctx["property_filters"][prop] = value
    filter_ctx["result"] = build_filter_json(_rebuild_spec(filter_ctx))


@when(parsers.parse('I build a filter with property "{prop}" equal to list \'{json_list}\''))
def when_equals_list(filter_ctx: dict[str, Any], prop: str, json_list: str) -> None:
    filter_ctx["property_filters"][prop] = json.loads(json_list)
    filter_ctx["result"] = build_filter_json(_rebuild_spec(filter_ctx))


@when(parsers.parse('I build a filter with exists properties \'{json_list}\''))
def when_exists(filter_ctx: dict[str, Any], json_list: str) -> None:
    filter_ctx["exists_properties"] = json.loads(json_list)
    filter_ctx["result"] = build_filter_json(_rebuild_spec(filter_ctx))


@when(parsers.parse('I build a filter with not-exists properties \'{json_list}\''))
def when_not_exists(filter_ctx: dict[str, Any], json_list: str) -> None:
    filter_ctx["not_exists_properties"] = json.loads(json_list)
    filter_ctx["result"] = build_filter_json(_rebuild_spec(filter_ctx))


@when(parsers.parse('I add exists properties \'{json_list}\''))
def when_add_exists(filter_ctx: dict[str, Any], json_list: str) -> None:
    filter_ctx["exists_properties"] = json.loads(json_list)
    filter_ctx["result"] = build_filter_json(_rebuild_spec(filter_ctx))


@when(parsers.parse('I build a filter with instance space "{space}"'))
def when_instance_space(filter_ctx: dict[str, Any], space: str) -> None:
    filter_ctx["instance_space"] = space
    filter_ctx["result"] = build_filter_json(_rebuild_spec(filter_ctx))


@when(parsers.parse('I build a filter with instance space list \'{json_list}\''))
def when_instance_space_list(filter_ctx: dict[str, Any], json_list: str) -> None:
    filter_ctx["instance_space"] = json.loads(json_list)
    filter_ctx["result"] = build_filter_json(_rebuild_spec(filter_ctx))


@when(parsers.parse('I build a filter with external id "{ext_id}"'))
def when_external_id(filter_ctx: dict[str, Any], ext_id: str) -> None:
    filter_ctx["external_id"] = ext_id
    filter_ctx["result"] = build_filter_json(_rebuild_spec(filter_ctx))


@when(parsers.parse('I build a filter with gt map \'{json_map}\''))
def when_gt(filter_ctx: dict[str, Any], json_map: str) -> None:
    filter_ctx["gt"] = json.loads(json_map)
    filter_ctx["result"] = build_filter_json(_rebuild_spec(filter_ctx))


@when(parsers.parse('I build a filter with gte map \'{json_map}\''))
def when_gte(filter_ctx: dict[str, Any], json_map: str) -> None:
    filter_ctx["gte"] = json.loads(json_map)
    filter_ctx["result"] = build_filter_json(_rebuild_spec(filter_ctx))


@when(parsers.parse('I add lt map \'{json_map}\''))
def when_add_lt(filter_ctx: dict[str, Any], json_map: str) -> None:
    filter_ctx["lt"] = json.loads(json_map)
    filter_ctx["result"] = build_filter_json(_rebuild_spec(filter_ctx))


@when("I build a filter with no conditions")
def when_empty(filter_ctx: dict[str, Any]) -> None:
    filter_ctx["result"] = build_filter_json(_rebuild_spec(filter_ctx))


@then("the filter JSON should be null")
def then_filter_null(filter_ctx: dict[str, Any]) -> None:
    assert filter_ctx["result"] is None


@then("the filter JSON should be:")
def then_filter_json(filter_ctx: dict[str, Any], docstring: str) -> None:
    expected = json.loads(docstring)
    assert filter_ctx["result"] == expected


@given(
    parsers.parse(
        "page_limit is {page_limit:d} and row_limit is {row_limit} and rows_yielded is {rows_yielded:d}"
    )
)
def given_limit(limit_ctx: dict[str, Any], page_limit: int, row_limit: str, rows_yielded: int) -> None:
    limit_ctx["page_limit"] = page_limit
    limit_ctx["row_limit"] = None if row_limit == "None" else int(row_limit)
    limit_ctx["rows_yielded"] = rows_yielded


@when("I compute the effective list request limit")
def when_effective_limit(limit_ctx: dict[str, Any]) -> None:
    limit_ctx["result"] = effective_list_request_limit(
        limit_ctx["page_limit"],
        limit_ctx["row_limit"],
        limit_ctx["rows_yielded"],
    )


@then(parsers.parse("the request limit should be {expected}"))
def then_request_limit(limit_ctx: dict[str, Any], expected: str) -> None:
    if expected == "null":
        assert limit_ctx["result"] is None
    else:
        assert limit_ctx["result"] == int(expected)


@given(
    parsers.parse(
        'an aggregate view "{external_id}" in space "{space}" version "{version}" '
        'for instance type "{instance_type}"'
    )
)
def given_agg_view(
    agg_ctx: dict[str, Any], external_id: str, space: str, version: str, instance_type: str
) -> None:
    agg_ctx["view_space"] = space
    agg_ctx["view_external_id"] = external_id
    agg_ctx["view_version"] = version
    agg_ctx["instance_type"] = instance_type


@given(
    parsers.parse(
        'a filter equals property "{prop}" to "{value}" on view "{external_id}" '
        'space "{space}" version "{version}"'
    )
)
def given_list_filter(
    agg_ctx: dict[str, Any], prop: str, value: str, external_id: str, space: str, version: str
) -> None:
    agg_ctx["filter_json"] = build_filter_json(
        FilterSpec(
            view_space=space,
            view_external_id=external_id,
            view_version=version,
            property_filters={prop: value},
        )
    )


def _table_rows(datatable: list) -> list[dict[str, str]]:  # type: ignore[type-arg]
    """Normalize pytest-bdd datatable to list of dicts."""
    if not datatable:
        return []
    if isinstance(datatable[0], dict):
        return list(datatable)
    headers = [str(h) for h in datatable[0]]
    return [dict(zip(headers, [str(c) for c in row], strict=False)) for row in datatable[1:]]


@when("I build an aggregate request with metrics:")
def when_agg_metrics(agg_ctx: dict[str, Any], datatable) -> None:  # type: ignore[no-untyped-def]
    metrics = [AggregateMetric(fn=row["fn"], property=row["property"]) for row in _table_rows(datatable)]
    agg_ctx["metrics"] = metrics
    agg_ctx["payload"] = build_aggregate_payload(
        AggregateRequestSpec(
            view_space=agg_ctx["view_space"],
            view_external_id=agg_ctx["view_external_id"],
            view_version=agg_ctx["view_version"],
            instance_type=agg_ctx["instance_type"],
            aggregates=metrics,
            filter_json=agg_ctx.get("filter_json"),
        )
    )


@when("I build an aggregate request with that filter and metrics:")
def when_agg_with_filter(agg_ctx: dict[str, Any], datatable) -> None:  # type: ignore[no-untyped-def]
    when_agg_metrics(agg_ctx, datatable)


@then("the aggregate payload aggregates should be:")
def then_agg_list(agg_ctx: dict[str, Any], docstring: str) -> None:
    expected = json.loads(docstring)
    assert agg_ctx["payload"]["aggregates"] == expected


@then(
    parsers.parse(
        'the aggregate payload should reference view space "{space}" '
        'externalId "{external_id}" version "{version}"'
    )
)
def then_agg_view(agg_ctx: dict[str, Any], space: str, external_id: str, version: str) -> None:
    view = agg_ctx["payload"]["view"]
    assert view == {"type": "view", "space": space, "externalId": external_id, "version": version}


@then("the aggregate payload should not use sources array")
def then_no_sources(agg_ctx: dict[str, Any]) -> None:
    assert "sources" not in agg_ctx["payload"]


@then("the aggregate payload filter should equal the list filter JSON")
def then_filter_reused(agg_ctx: dict[str, Any]) -> None:
    assert agg_ctx["payload"]["filter"] == agg_ctx["filter_json"]


@given("an aggregate response:")
def given_agg_response(agg_ctx: dict[str, Any], docstring: str) -> None:
    agg_ctx["response"] = json.loads(docstring)


@when("I parse the aggregate count")
def when_parse_count(agg_ctx: dict[str, Any]) -> None:
    agg_ctx["count"] = parse_aggregate_count(agg_ctx["response"])


@when("I parse the metric aggregates")
def when_parse_metrics(agg_ctx: dict[str, Any]) -> None:
    agg_ctx["metrics_parsed"] = parse_metric_aggregates(agg_ctx["response"])


@then(parsers.parse("the count value should be {value:d}"))
def then_count(agg_ctx: dict[str, Any], value: int) -> None:
    assert agg_ctx["count"] == value


@then(parsers.parse('the metric ("{fn}", "{prop}") should be {value:d}'))
def then_metric(agg_ctx: dict[str, Any], fn: str, prop: str, value: int) -> None:
    assert agg_ctx["metrics_parsed"][(fn, prop)] == value
