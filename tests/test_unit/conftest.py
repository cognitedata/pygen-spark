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
        properties={
            "name": dm.Text(),
            "description": dm.Text(),
            "boat_guid": dm.Int64(),
            "mmsi_country": dm.Text(),
        },
    )


@pytest.fixture
def sample_data_model(sample_view: dm.View) -> dm.DataModel[dm.View]:
    """Sample data model for unit testing."""
    return dm.DataModel(
        space="test_space",
        external_id="test_model",
        version="v1",
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
    return SparkMultiAPIGenerator(
        top_level_package="test_package",
        client_name="TestClient",
        data_models=[sample_data_model],
        instance_space="test_space",
    )

