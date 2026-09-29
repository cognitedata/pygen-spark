"""UDTF parameter registries keep generated UDTF signatures and view SQL aligned.

Downstream Unity Catalog registration builds the function signature from this registry, so every
named argument the view SQL passes must come from it (``UNRECOGNIZED_PARAMETER_NAME`` otherwise).
"""

from __future__ import annotations

import ast
import re

import pytest
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from pyspark.sql.types import LongType, StringType

SECRET_PARAMETERS = ["client_id", "client_secret", "tenant_id", "cdf_cluster", "project"]
PROPERTY_NAMES = ["name", "description", "boat_guid"]
EXPECTED_PUSHDOWN_NAMES = [
    "instance_space",
    "external_id",
    "_exists",
    "_not_exists",
    "_gt",
    "_gte",
    "_lt",
    "_lte",
    "_row_limit",
    "_query_mode",
    "_aggregates",
    "_group_by",
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
            "name": dm.Text(),  # type: ignore[dict-item]
            "description": dm.Text(),  # type: ignore[dict-item]
            "boat_guid": dm.Text(),  # type: ignore[dict-item]
        },
        filter=None,
        implements=None,
        writable=False,
        used_for="node",
        is_global=False,
    )


@pytest.fixture
def generator(mock_cognite_client: CogniteClient, small_boat_view: dm.View):  # type: ignore[no-untyped-def]
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
    return SparkMultiAPIGenerator(  # type: ignore[call-arg]
        top_level_package="test_package",  # type: ignore[arg-type]
        client_name="TestClient",  # type: ignore[arg-type]
        data_models=[model],  # type: ignore[arg-type]
        default_instance_space="sailboat",  # type: ignore[arg-type]
    )


def _method_params(code: str, class_name: str, method_name: str) -> list[str]:
    tree = ast.parse(code)
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == method_name:
                    return [arg.arg for arg in item.args.args if arg.arg != "self"]
    raise AssertionError(f"{class_name}.{method_name} not found in generated code")


def _named_args(sql: str) -> list[str]:
    return re.findall(r"(\w+)\s*=>", sql)


def test_registry_lists_pushdown_parameters_in_signature_order() -> None:
    from cognite.pygen_spark.udtf_parameters import data_model_pushdown_parameters

    assert data_model_pushdown_parameters.names == EXPECTED_PUSHDOWN_NAMES


def test_registry_types_row_limit_as_long_and_rest_as_string() -> None:
    from cognite.pygen_spark.udtf_parameters import data_model_pushdown_parameters

    by_name = data_model_pushdown_parameters.by_name
    assert isinstance(by_name["_row_limit"].spark_type, LongType)
    for name in EXPECTED_PUSHDOWN_NAMES:
        if name != "_row_limit":
            assert isinstance(by_name[name].spark_type, StringType), name


def test_registry_is_exported_from_package() -> None:
    import cognite.pygen_spark as pygen_spark

    assert pygen_spark.data_model_pushdown_parameters.names == EXPECTED_PUSHDOWN_NAMES


def test_eval_signature_is_secrets_properties_then_registry(generator, small_boat_view: dm.View) -> None:  # type: ignore[no-untyped-def]
    from cognite.pygen_spark.udtf_parameters import data_model_pushdown_parameters

    code = generator.generate_udtf(small_boat_view, include_analyze=True, use_udtf_decorator=False)
    assert _method_params(code, "SmallBoatUDTF", "eval") == (
        SECRET_PARAMETERS + PROPERTY_NAMES + data_model_pushdown_parameters.names + ["base_url"]
    )


def test_analyze_signature_matches_eval(generator, small_boat_view: dm.View) -> None:  # type: ignore[no-untyped-def]
    code = generator.generate_udtf(small_boat_view, include_analyze=True, use_udtf_decorator=False)
    assert _method_params(code, "SmallBoatUDTF", "analyze") == _method_params(code, "SmallBoatUDTF", "eval")


def test_view_sql_named_args_match_eval_signature(generator, small_boat_view: dm.View) -> None:  # type: ignore[no-untyped-def]
    code = generator.generate_udtf(small_boat_view, include_analyze=True, use_udtf_decorator=False)
    sql = generator.generate_view_sql(view=small_boat_view, secret_scope="cdf_sailboat_sailboat")
    assert _named_args(sql) == _method_params(code, "SmallBoatUDTF", "eval")
