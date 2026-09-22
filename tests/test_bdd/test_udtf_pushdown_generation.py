"""Unit tests that generated UDTF code includes filter / LIMIT / aggregate pushdown."""

from __future__ import annotations

import pytest
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

pytest.importorskip("pyspark")

from cognite.pygen_spark.udtf_generator import SparkMultiAPIGenerator


@pytest.fixture
def lims_view() -> dm.View:
    return dm.View(
        space="sp-lims",
        external_id="LimsResults",
        version="v1",
        created_time=1,
        last_updated_time=2,
        name="",
        description="",
        properties={
            "TestSeqNumber": dm.Text(),  # type: ignore[dict-item]
            "DilutionFactor": dm.Float64(),  # type: ignore[dict-item]
        },
        filter=None,
        implements=None,
        writable=False,
        used_for="node",
        is_global=False,
    )


@pytest.fixture
def generator(mock_cognite_client: CogniteClient, lims_view: dm.View) -> SparkMultiAPIGenerator:
    model = dm.DataModel(
        space="sp-lims",
        external_id="lims",
        version="v1",
        created_time=1,
        last_updated_time=2,
        name=None,
        description=None,
        is_global=False,
        views=[lims_view],
    )
    return SparkMultiAPIGenerator(  # type: ignore[call-arg]
        top_level_package="test_package",  # type: ignore[arg-type]
        client_name="TestClient",  # type: ignore[arg-type]
        data_models=[model],  # type: ignore[arg-type]
        default_instance_space="sp-lims",  # type: ignore[arg-type]
    )


def test_generated_udtf_includes_pushdown_params(
    generator: SparkMultiAPIGenerator, lims_view: dm.View
) -> None:
    code = generator.generate_udtf(lims_view, include_analyze=True, use_udtf_decorator=False)
    assert "_exists" in code
    assert "_not_exists" in code
    assert "_row_limit" in code
    assert "_query_mode" in code
    assert "_aggregates" in code
    assert "exists_properties" in code
    assert '[instance_type, "space"]' in code
    assert "/models/instances/aggregate" in code
    assert "effective_row_limit" in code
    assert "rows_yielded" in code


def test_generated_view_sql_includes_pushdown_nulls(
    generator: SparkMultiAPIGenerator, lims_view: dm.View
) -> None:
    sql = generator.generate_view_sql(view=lims_view, secret_scope="test_scope")
    assert "_exists => NULL" in sql
    assert "_row_limit => NULL" in sql
    assert "instance_space => NULL" in sql
