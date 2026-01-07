"""Unit test fixtures for pygen-spark."""

from __future__ import annotations

from pathlib import Path

import pytest
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.pygen_spark import SparkUDTFGenerator
from cognite.pygen_spark.udtf_generator import SparkMultiAPIGenerator


@pytest.fixture
def sample_view() -> dm.View:
    """Sample view for unit testing."""
    return dm.View(
        space="test_space",
        external_id="SmallBoat",
        version="v1",
        created_time=1,
        last_updated_time=2,
        name="",
        description="",
        properties={
            "name": dm.Text(),  # type: ignore[dict-item]
            "description": dm.Text(),  # type: ignore[dict-item]
            "boat_guid": dm.Int64(),  # type: ignore[dict-item]
            "mmsi_country": dm.Text(),  # type: ignore[dict-item]
        },
        filter=None,
        implements=None,
        writable=False,
        used_for="all",
        is_global=False,
    )


@pytest.fixture
def sample_data_model(sample_view: dm.View) -> dm.DataModel[dm.View]:
    """Sample data model for unit testing."""
    return dm.DataModel(
        space="test_space",
        external_id="test_model",
        version="v1",
        created_time=1,
        last_updated_time=2,
        name=None,
        description=None,
        is_global=False,
        views=[sample_view],
    )


@pytest.fixture
def spark_udtf_generator(
    mock_cognite_client: CogniteClient,
    temp_output_dir: Path,
    sample_data_model: dm.DataModel[dm.View],
) -> SparkUDTFGenerator:
    """SparkUDTFGenerator instance for testing."""
    return SparkUDTFGenerator(
        client=mock_cognite_client,
        output_dir=temp_output_dir,
        data_model=sample_data_model,
    )


@pytest.fixture
def spark_multi_api_generator(
    mock_cognite_client: CogniteClient,
    temp_output_dir: Path,
    sample_data_model: dm.DataModel[dm.View],
) -> SparkMultiAPIGenerator:
    """SparkMultiAPIGenerator instance for testing."""
    return SparkMultiAPIGenerator(  # type: ignore
        top_level_package="test_package",  # type: ignore[arg-type]
        client_name="TestClient",  # type: ignore[arg-type]
        data_models=[sample_data_model],  # type: ignore[arg-type]
        default_instance_space="test_space",  # type: ignore[arg-type]
    )

