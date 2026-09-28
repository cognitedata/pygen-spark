"""Runtime ``base_url`` (Private Link, dedicated clusters) for every generated UDTF.

The TOML ``base_url`` is stored in Secret Manager and passed as the trailing ``base_url`` argument.
API requests go to it; OAuth scopes still derive from ``cdf_cluster``. An empty value falls back to
the public ``https://{cdf_cluster}.cognitedata.com`` URL.
"""

from __future__ import annotations

import ast
import re
import textwrap
from collections.abc import Callable

import pytest
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

pytest.importorskip("pyspark")

from cognite.pygen_spark.audit import cdf_audit_http_template_context

TIME_SERIES_TEMPLATES = [
    "time_series_datapoints_udtf.py.jinja",
    "time_series_datapoints_detailed_udtf.py.jinja",
    "time_series_latest_datapoints_udtf.py.jinja",
    "time_series_sql_udtf.py.jinja",
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
        properties={"name": dm.Text()},  # type: ignore[dict-item]
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


def _render_time_series(generator, template_name: str) -> str:  # type: ignore[no-untyped-def]
    return str(
        generator.env.get_template(template_name).render(
            include_analyze=True,
            use_udtf_decorator=False,
            **cdf_audit_http_template_context(audit_component="UnityCatalogUDTF", audit_tail="Databricks"),
        )
    )


def _method_params(code: str, method_name: str) -> list[list[str]]:
    """Parameter names of every method with this name in the generated code."""
    found: list[list[str]] = []
    for node in ast.walk(ast.parse(code)):
        if isinstance(node, ast.FunctionDef) and node.name == method_name:
            found.append([arg.arg for arg in node.args.args if arg.arg != "self"])
    return found


def _generated_function(code: str, name: str) -> Callable[..., object]:
    for node in ast.walk(ast.parse(code)):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            source = textwrap.dedent(ast.get_source_segment(code, node) or "")
            namespace: dict[str, object] = {}
            exec(compile(source, f"<generated {name}>", "exec"), namespace)
            function = namespace[name]
            assert callable(function)
            return function
    raise AssertionError(f"{name} not found in generated code")


def _all_generated(generator, view: dm.View) -> dict[str, str]:  # type: ignore[no-untyped-def]
    codes = {"data_model": generator.generate_udtf(view, include_analyze=True, use_udtf_decorator=False)}
    for template_name in TIME_SERIES_TEMPLATES:
        codes[template_name] = _render_time_series(generator, template_name)
    return codes


def test_base_url_parameter_is_exported_as_string() -> None:
    from pyspark.sql.types import StringType

    from cognite.pygen_spark.udtf_parameters import base_url_parameter

    assert base_url_parameter.name == "base_url"
    assert isinstance(base_url_parameter.spark_type, StringType)


def test_every_udtf_takes_base_url_as_last_parameter(generator, small_boat_view: dm.View) -> None:  # type: ignore[no-untyped-def]
    for label, code in _all_generated(generator, small_boat_view).items():
        signatures = _method_params(code, "eval") + _method_params(code, "analyze")
        assert signatures, label
        for params in signatures:
            assert params[-1] == "base_url", f"{label}: {params}"


def test_view_sql_passes_base_url_secret_last(generator, small_boat_view: dm.View) -> None:  # type: ignore[no-untyped-def]
    sql = generator.generate_view_sql(view=small_boat_view, secret_scope="cdf_sailboat_sailboat")
    assert re.findall(r"(\w+)\s*=>", sql)[-1] == "base_url"
    assert "base_url => SECRET('cdf_sailboat_sailboat', 'base_url')" in sql


def test_no_udtf_hardcodes_the_public_api_url(generator, small_boat_view: dm.View) -> None:  # type: ignore[no-untyped-def]
    for label, code in _all_generated(generator, small_boat_view).items():
        assert 'base_url = f"https://{cdf_cluster_str}.cognitedata.com"' not in code, label
        assert "_resolve_base_url(" in code, label


def test_token_scope_still_uses_cdf_cluster(generator, small_boat_view: dm.View) -> None:  # type: ignore[no-untyped-def]
    for label, code in _all_generated(generator, small_boat_view).items():
        assert 'scope = f"https://{cdf_cluster}.cognitedata.com/.default"' in code, label


@pytest.mark.parametrize(
    ("base_url", "expected"),
    [
        (None, "https://az-xyz-001.cognitedata.com"),
        ("", "https://az-xyz-001.cognitedata.com"),
        ("   ", "https://az-xyz-001.cognitedata.com"),
        ("https://p001.plink.az-xyz-001.cognitedata.com", "https://p001.plink.az-xyz-001.cognitedata.com"),
        (" https://p001.plink.az-xyz-001.cognitedata.com/ ", "https://p001.plink.az-xyz-001.cognitedata.com"),
    ],
)
def test_generated_resolver_prefers_base_url_and_falls_back_to_cluster(
    generator,  # type: ignore[no-untyped-def]
    small_boat_view: dm.View,
    base_url: str | None,
    expected: str,
) -> None:
    for label, code in _all_generated(generator, small_boat_view).items():
        resolve = _generated_function(code, "_resolve_base_url")
        assert resolve(base_url, "az-xyz-001") == expected, label
