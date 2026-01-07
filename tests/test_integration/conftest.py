"""Integration test fixtures for pygen-spark."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.testing import monkeypatch_cognite_client

from cognite.pygen_spark import SparkUDTFGenerator


@pytest.fixture()
def mock_cognite_client() -> Iterable[CogniteClient]:
    """Mock CogniteClient for integration testing."""
    with monkeypatch_cognite_client() as m:
        # Setup default mock responses
        yield m


@pytest.fixture
def mock_workspace_client() -> MagicMock:
    """Mock WorkspaceClient for integration testing."""
    mock = MagicMock()
    # Setup default mock responses for workspace operations
    return mock


@pytest.fixture
def sample_sailboat_view() -> dm.View:
    """Sample sailboat view matching notebook data."""
    return dm.View(
        space="sailboat",
        external_id="SmallBoat",
        version="v1",
        created_time=1,
        last_updated_time=2,
        name="",
        description="",
        properties={  # type: ignore[dict-item]
            "name": dm.Text(),
            "description": dm.Text(),
            "boat_guid": dm.Int64(),
            "mmsi_country": dm.Text(),
        },
        filter=None,
        implements=None,
        writable=False,
        used_for="all",
        is_global=False,
    )


@pytest.fixture
def sample_nmea_time_series_view() -> dm.View:
    """Sample NMEA time series view matching notebook data."""
    return dm.View(
        space="sailboat",
        external_id="NmeaTimeSeries",
        version="v1",
        created_time=1,
        last_updated_time=2,
        name="",
        description="",
        properties={  # type: ignore[dict-item]
            "mmsi": dm.Text(),
            "value": dm.Float64(),
        },
        filter=None,
        implements=None,
        writable=False,
        used_for="all",
        is_global=False,
    )


@pytest.fixture
def sailboat_data_model(
    sample_sailboat_view: dm.View,
    sample_nmea_time_series_view: dm.View,
) -> dm.DataModel[dm.View]:
    """Data model matching notebook structure."""
    return dm.DataModel(
        space="sailboat",
        external_id="sailboat",
        version="v1",
        created_time=1,
        last_updated_time=2,
        name=None,
        description=None,
        is_global=False,
        views=[sample_sailboat_view, sample_nmea_time_series_view],
    )


@pytest.fixture
def temp_output_dir(tmp_path: Path) -> Path:
    """Temporary directory for integration test output."""
    output_dir = tmp_path / "udtf_output"
    output_dir.mkdir()
    return output_dir


@pytest.fixture
def spark_udtf_generator(
    mock_cognite_client: CogniteClient,
    temp_output_dir: Path,
    sailboat_data_model: dm.DataModel[dm.View],
) -> SparkUDTFGenerator:
    """SparkUDTFGenerator instance for integration testing."""
    return SparkUDTFGenerator(
        client=mock_cognite_client,
        output_dir=temp_output_dir,
        data_model=sailboat_data_model,
    )

