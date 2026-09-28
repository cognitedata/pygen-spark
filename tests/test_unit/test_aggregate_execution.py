"""Execute the generated UDTF aggregate path as Unity Catalog does.

Unity Catalog runs ``eval()`` without the module-level ``pyspark.sql.types`` imports, so the aggregate
branch must not call ``outputSchema()`` (``NameError: name 'StructField' is not defined`` on Databricks).
Yielded values must also match the output column types (COUNT into the STRING ``external_id`` column).
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

pytest.importorskip("pyspark")

MODULE_LEVEL_SPARK_NAMES = [
    "StructType",
    "StructField",
    "StringType",
    "LongType",
    "DoubleType",
    "BooleanType",
    "TimestampType",
    "ArrayType",
]


@pytest.fixture
def small_boat_view() -> dm.View:
    return dm.View(
        space="sailboat",
        external_id="SmallBoat",
        version="v1",
        created_time=1,
        last_updated_time=2,
        name="",
        description="",
        properties={
            "name": dm.MappedProperty(
                container=dm.ContainerId("cdf_cdm", "CogniteDescribable"),
                container_property_identifier="name",
                type=dm.Text(),
                nullable=True,
                immutable=False,
                auto_increment=False,
            ),
            "sourceCreatedTime": dm.MappedProperty(
                container=dm.ContainerId("cdf_cdm", "CogniteSourceable"),
                container_property_identifier="sourceCreatedTime",
                type=dm.Timestamp(),
                nullable=True,
                immutable=False,
                auto_increment=False,
            ),
            "aph_tod": dm.MappedProperty(
                container=dm.ContainerId("sailboat", "ORCCertificate"),
                container_property_identifier="aph_tod",
                type=dm.Float32(),
                nullable=True,
                immutable=False,
                auto_increment=False,
            ),
        },
        filter=None,
        implements=None,
        writable=True,
        used_for="node",
        is_global=False,
    )


@pytest.fixture
def udtf_class(mock_cognite_client: CogniteClient, small_boat_view: dm.View) -> type:
    from cognite.pygen_spark.udtf_generator import SparkMultiAPIGenerator

    model = dm.DataModel(
        space="sailboat",
        external_id="sailboat",
        version="v1",
        created_time=1,
        last_updated_time=2,
        name=None,
        description=None,
        is_global=False,
        views=[small_boat_view],
    )
    generator = SparkMultiAPIGenerator(  # type: ignore[call-arg]
        top_level_package="test_package",  # type: ignore[arg-type]
        client_name="TestClient",  # type: ignore[arg-type]
        data_models=[model],  # type: ignore[arg-type]
        default_instance_space="sailboat",  # type: ignore[arg-type]
    )
    code = generator.generate_udtf(small_boat_view, include_analyze=True, use_udtf_decorator=False)
    namespace: dict[str, object] = {"__name__": "generated_small_boat_udtf"}
    exec(compile(code, "<generated SmallBoat UDTF>", "exec"), namespace)
    cls = namespace["SmallBoatUDTF"]
    assert isinstance(cls, type)
    cls.expected_fields = [f.name for f in cls.outputSchema().fields]  # type: ignore[attr-defined]
    # Unity Catalog does not execute module-level code at query time
    for name in MODULE_LEVEL_SPARK_NAMES:
        namespace.pop(name, None)
    return cls


def _response(payload: dict[str, object]) -> MagicMock:
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


def _run_aggregate(udtf_class: type, aggregates: list[dict[str, str]], values: list[dict[str, object]]) -> list[tuple]:
    token = _response({"access_token": "token", "expires_in": 3600})
    return _run_aggregate_with_request(udtf_class, aggregates, values)[0]


def _run_aggregate_with_request(
    udtf_class: type, aggregates: list[dict[str, str]], values: list[dict[str, object]]
) -> tuple[list[tuple], dict[str, object]]:
    token = _response({"access_token": "token", "expires_in": 3600})
    aggregate = _response({"items": [{"instanceType": "node", "aggregates": values}]})
    with (
        patch("requests.post", return_value=token),
        patch("requests.request", return_value=aggregate) as request,
    ):
        rows: Iterator[tuple] = udtf_class().eval(
            client_id="id",
            client_secret="secret",
            tenant_id="tenant",
            cdf_cluster="westeurope-1",
            project="cognite-cogsail",
            instance_space="inst_sailboat_fleet_b",
            _query_mode="aggregate",
            _aggregates=json.dumps(aggregates),
        )
        collected = list(rows)
    return collected, request.call_args.kwargs["json"]


def test_list_mode_runs_without_module_level_spark_imports(udtf_class: type) -> None:
    token = _response({"access_token": "token", "expires_in": 3600})
    listed = _response(
        {
            "items": [
                {
                    "space": "inst_sailboat_fleet_a",
                    "externalId": "seed_small_boat_fleet_a_01",
                    "createdTime": 1790000000000,
                    "lastUpdatedTime": 1790000000000,
                    "properties": {
                        "sailboat": {"SmallBoat/v1": {"name": "Seed Fleet A 01", "sourceCreatedTime": None}}
                    },
                }
            ]
        }
    )
    with patch("requests.post", return_value=token), patch("requests.request", return_value=listed):
        rows = list(
            udtf_class().eval(
                client_id="id",
                client_secret="secret",
                tenant_id="tenant",
                cdf_cluster="westeurope-1",
                project="cognite-cogsail",
                instance_space="inst_sailboat_fleet_a",
            )
        )
    fields = udtf_class.expected_fields  # type: ignore[attr-defined]
    assert len(rows) == 1
    assert len(rows[0]) == len(fields)
    assert rows[0][fields.index("external_id")] == "seed_small_boat_fleet_a_01"
    assert rows[0][fields.index("name")] == "Seed Fleet A 01"


def test_count_runs_without_module_level_spark_imports(udtf_class: type) -> None:
    rows = _run_aggregate(
        udtf_class,
        [{"fn": "count", "property": "externalId"}],
        [{"aggregate": "count", "property": "externalId", "value": 2}],
    )
    fields = udtf_class.expected_fields  # type: ignore[attr-defined]
    assert len(rows) == 1
    assert len(rows[0]) == len(fields)
    assert rows[0][fields.index("external_id")] == "2"


def test_min_max_numeric_sends_bare_property_names_and_typed_values(udtf_class: type) -> None:
    rows, request_json = _run_aggregate_with_request(
        udtf_class,
        [{"fn": "min", "property": "aph_tod"}, {"fn": "max", "property": "aph_tod"}],
        [
            {"aggregate": "min", "property": "aph_tod", "value": 480.5},
            {"aggregate": "max", "property": "aph_tod", "value": 530.25},
        ],
    )
    # CDF rejects a property path array here: "Invalid field - min.property - expected string but got an array"
    assert request_json["aggregates"] == [{"min": {"property": "aph_tod"}}, {"max": {"property": "aph_tod"}}]
    fields = udtf_class.expected_fields  # type: ignore[attr-defined]
    assert isinstance(rows[0][fields.index("aph_tod")], float)


@pytest.mark.parametrize("prop", ["name", "sourceCreatedTime"])
def test_min_max_on_non_numeric_property_fails_with_guidance(udtf_class: type, prop: str) -> None:
    # CDF: "Expected property to be of a numerical type" — fail before the request, with a clear message
    with pytest.raises(ValueError, match="numeric"):
        _run_aggregate(udtf_class, [{"fn": "min", "property": prop}], [])
