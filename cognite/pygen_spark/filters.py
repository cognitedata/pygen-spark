"""CDF FilterDefinition builders for data-model UDTF pushdown.

Produces REST-compatible filter JSON for ``POST /models/instances/list`` and
``POST /models/instances/aggregate``. Generated UDTFs inline equivalent logic
(no runtime dependency on this module on Spark workers).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field

InstanceType = Literal["node", "edge"]
AggregateFn = Literal["count", "min", "max"]


class FilterSpec(BaseModel):
    """Specification for building a CDF FilterDefinition JSON object."""

    view_space: str
    view_external_id: str
    view_version: str
    instance_type: InstanceType = "node"
    property_is_array: dict[str, bool] = Field(default_factory=dict)
    property_filters: dict[str, object] = Field(default_factory=dict)
    exists_properties: list[str] = Field(default_factory=list)
    not_exists_properties: list[str] = Field(default_factory=list)
    instance_space: str | list[str] | None = None
    external_id: str | list[str] | None = None
    gt: dict[str, object] = Field(default_factory=dict)
    gte: dict[str, object] = Field(default_factory=dict)
    lt: dict[str, object] = Field(default_factory=dict)
    lte: dict[str, object] = Field(default_factory=dict)

    @property
    def view_property_prefix(self) -> list[str]:
        """Property path prefix for view properties: [space, ViewId/version]."""
        return [self.view_space, f"{self.view_external_id}/{self.view_version}"]

    def view_property_ref(self, prop_name: str) -> list[str]:
        """Full DMS property path for a view property."""
        return [*self.view_property_prefix, prop_name]

    def identity_property_ref(self, identity_field: Literal["space", "externalId"]) -> list[str]:
        """Instance identity path: [node|edge, space|externalId]."""
        return [self.instance_type, identity_field]


class AggregateMetric(BaseModel):
    """One metric aggregate for instances/aggregate."""

    fn: AggregateFn
    property: str = Field(description="externalId for count; view property name for min/max")


class AggregateRequestSpec(BaseModel):
    """Specification for building an instances/aggregate request payload."""

    view_space: str
    view_external_id: str
    view_version: str
    instance_type: InstanceType = "node"
    aggregates: list[AggregateMetric] = Field(default_factory=list)
    filter_json: dict[str, Any] | None = None
    group_by: list[str] = Field(default_factory=list)
    limit: int = 1000
    operator: Literal["AND", "OR"] = "AND"


def coerce_filter_value(value: object) -> object:
    """Normalize filter values to SDK-like shapes (JSON list strings, datetimes)."""
    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        if value.tzinfo is not None:
            dt = value.astimezone(timezone.utc)
        return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    # Duck-typed datetime-like (e.g. Spark / pandas timestamps)
    if hasattr(value, "timestamp") or hasattr(value, "isoformat"):
        try:
            iso = getattr(value, "isoformat", None)
            if callable(iso):
                dt_obj = value
                tzinfo = getattr(dt_obj, "tzinfo", None)
                if tzinfo is None and hasattr(dt_obj, "replace"):
                    dt_obj = dt_obj.replace(tzinfo=timezone.utc)
                elif tzinfo is not None and hasattr(dt_obj, "astimezone"):
                    dt_obj = dt_obj.astimezone(timezone.utc)
                result = iso(timespec="milliseconds")
                return str(result).replace("+00:00", "Z")
            strftime = getattr(value, "strftime", None)
            if callable(strftime):
                return strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        except (AttributeError, TypeError):
            pass

    if hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day"):
        try:
            year = int(value.year)
            month = int(value.month)
            day = int(value.day)
            if hasattr(value, "hour"):
                dt = datetime(
                    year,
                    month,
                    day,
                    int(value.hour),
                    int(getattr(value, "minute", 0)),
                    int(getattr(value, "second", 0)),
                    int(getattr(value, "microsecond", 0)),
                )
            else:
                dt = datetime(year, month, day)
            if not hasattr(value, "tzinfo") or getattr(value, "tzinfo", None) is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        except (AttributeError, TypeError, ValueError):
            pass

    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            try:
                parsed = json.loads(stripped)
                if isinstance(parsed, list):
                    return parsed
            except (json.JSONDecodeError, ValueError):
                pass
    return value


def parse_json_dict_or_list(value: object | None) -> object | None:
    """Parse optional JSON string into dict/list, or return value as-is."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return json.loads(stripped)
        except (json.JSONDecodeError, ValueError):
            return value
    return value


def parse_property_name_list(value: object | None) -> list[str]:
    """Parse exists/_not_exists param into a list of property names."""
    parsed = parse_json_dict_or_list(value)
    if parsed is None:
        return []
    if isinstance(parsed, str):
        return [parsed] if parsed else []
    if isinstance(parsed, list):
        return [str(item) for item in parsed if item is not None]
    return []


def parse_range_map(value: object | None) -> dict[str, object]:
    """Parse _gt/_gte/_lt/_lte JSON map of property -> value."""
    parsed = parse_json_dict_or_list(value)
    if not isinstance(parsed, dict):
        return {}
    return {str(k): v for k, v in parsed.items() if v is not None}


def _equals_or_in(property_ref: list[str], prop_value: object) -> dict[str, Any]:
    if isinstance(prop_value, list):
        return {"in": {"property": property_ref, "values": prop_value}}
    return {"equals": {"property": property_ref, "value": prop_value}}


def build_filter_json(spec: FilterSpec) -> dict[str, Any] | None:
    """Build CDF FilterDefinition JSON from a FilterSpec.

    Returns:
        Filter JSON dict, or None when no active conditions.
    """
    conditions: list[dict[str, Any]] = []

    for prop_name, raw_value in spec.property_filters.items():
        if raw_value is None:
            continue
        prop_value = coerce_filter_value(raw_value)
        property_ref = spec.view_property_ref(prop_name)
        is_array = spec.property_is_array.get(prop_name, False)
        if is_array:
            values = prop_value if isinstance(prop_value, list) else [prop_value]
            conditions.append({"containsAny": {"property": property_ref, "values": values}})
        else:
            conditions.append(_equals_or_in(property_ref, prop_value))

    for prop_name in spec.exists_properties:
        conditions.append({"exists": {"property": spec.view_property_ref(prop_name)}})

    for prop_name in spec.not_exists_properties:
        conditions.append({"not": {"exists": {"property": spec.view_property_ref(prop_name)}}})

    if spec.instance_space is not None:
        space_value = coerce_filter_value(spec.instance_space)
        conditions.append(_equals_or_in(spec.identity_property_ref("space"), space_value))

    if spec.external_id is not None:
        ext_value = coerce_filter_value(spec.external_id)
        conditions.append(_equals_or_in(spec.identity_property_ref("externalId"), ext_value))

    range_buckets: list[tuple[str, dict[str, object]]] = [
        ("gt", spec.gt),
        ("gte", spec.gte),
        ("lt", spec.lt),
        ("lte", spec.lte),
    ]
    # Merge range ops per property into a single range filter when possible
    range_by_prop: dict[str, dict[str, object]] = {}
    for op, mapping in range_buckets:
        for prop_name, raw_value in mapping.items():
            if raw_value is None:
                continue
            range_by_prop.setdefault(prop_name, {})[op] = coerce_filter_value(raw_value)
    for prop_name, range_body in range_by_prop.items():
        conditions.append({"range": {"property": spec.view_property_ref(prop_name), **range_body}})

    if not conditions:
        return None
    if len(conditions) == 1:
        return conditions[0]
    return {"and": conditions}


def build_aggregate_payload(spec: AggregateRequestSpec) -> dict[str, Any]:
    """Build REST payload for POST /models/instances/aggregate."""
    if len(spec.aggregates) > 5:
        raise ValueError("instances/aggregate allows at most 5 aggregates per request")

    aggregates_payload: list[dict[str, Any]] = []
    for metric in spec.aggregates:
        if metric.fn == "count" and metric.property in {"externalId", "external_id"}:
            aggregates_payload.append({"count": {"property": "externalId"}})
        else:
            property_ref = [
                spec.view_space,
                f"{spec.view_external_id}/{spec.view_version}",
                metric.property,
            ]
            aggregates_payload.append({metric.fn: {"property": property_ref}})

    payload: dict[str, Any] = {
        "view": {
            "type": "view",
            "space": spec.view_space,
            "externalId": spec.view_external_id,
            "version": spec.view_version,
        },
        "instanceType": spec.instance_type,
        "query": "",
        "aggregates": aggregates_payload,
        "limit": spec.limit,
        "operator": spec.operator,
    }
    if spec.filter_json:
        payload["filter"] = spec.filter_json
    if spec.group_by:
        payload["groupBy"] = spec.group_by
    return payload


def _aggregate_property_name(property_value: object) -> str:
    """Normalize CDF aggregate response property to a simple name.

    View-property aggregates return a path list ``[space, View/version, prop]``;
    count uses the string shorthand ``externalId``.
    """
    if isinstance(property_value, list) and property_value:
        return str(property_value[-1])
    if property_value is None:
        return ""
    return str(property_value)


def parse_metric_aggregates(response_data: dict[str, Any]) -> dict[tuple[str, str], object]:
    """Parse ungrouped items[0].aggregates into (aggregate, property) -> value."""
    result: dict[tuple[str, str], object] = {}
    items = response_data.get("items") or []
    if not items:
        return result
    for agg in items[0].get("aggregates") or []:
        key = (str(agg.get("aggregate")), _aggregate_property_name(agg.get("property")))
        if key[0] and key[1]:
            result[key] = agg.get("value")
    return result


def parse_aggregate_count(response_data: dict[str, Any]) -> int:
    """Parse COUNT aggregate value (property externalId)."""
    metrics = parse_metric_aggregates(response_data)
    val = metrics.get(("count", "externalId"))
    if val is None:
        return 0
    if isinstance(val, bool):
        return int(val)
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str):
        return int(val)
    return int(str(val))


def effective_list_request_limit(page_limit: int, row_limit: int | None, rows_yielded: int) -> int | None:
    """Compute instances/list ``limit`` for the next page, or None to stop.

    Args:
        page_limit: Adaptive page size from codegen.
        row_limit: Optional SQL LIMIT pushdown (_row_limit).
        rows_yielded: Rows already yielded.

    Returns:
        Next request limit, or None when no more rows should be fetched.
    """
    if row_limit is None:
        return page_limit
    remaining = row_limit - rows_yielded
    if remaining <= 0:
        return None
    return min(page_limit, remaining)


def filter_spec_from_udtf_params(
    *,
    view_space: str,
    view_external_id: str,
    view_version: str,
    instance_type: InstanceType,
    property_is_array: dict[str, bool],
    property_filters: dict[str, object],
    exists: object | None = None,
    not_exists: object | None = None,
    instance_space: object | None = None,
    external_id: object | None = None,
    gt: object | None = None,
    gte: object | None = None,
    lt: object | None = None,
    lte: object | None = None,
) -> FilterSpec:
    """Build FilterSpec from generated UDTF eval() parameters."""
    space_parsed = parse_json_dict_or_list(instance_space)
    if isinstance(space_parsed, list) and len(space_parsed) == 1:
        space_parsed = space_parsed[0]
    ext_parsed = parse_json_dict_or_list(external_id)
    if isinstance(ext_parsed, list) and len(ext_parsed) == 1:
        ext_parsed = ext_parsed[0]

    return FilterSpec(
        view_space=view_space,
        view_external_id=view_external_id,
        view_version=view_version,
        instance_type=instance_type,
        property_is_array=property_is_array,
        property_filters={k: v for k, v in property_filters.items() if v is not None},
        exists_properties=parse_property_name_list(exists),
        not_exists_properties=parse_property_name_list(not_exists),
        instance_space=space_parsed if space_parsed is not None else None,  # type: ignore[arg-type]
        external_id=ext_parsed if ext_parsed is not None else None,  # type: ignore[arg-type]
        gt=parse_range_map(gt),
        gte=parse_range_map(gte),
        lt=parse_range_map(lt),
        lte=parse_range_map(lte),
    )
