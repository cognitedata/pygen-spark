"""Root test configuration and shared fixtures for pygen-spark tests."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import pytest
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.testing import monkeypatch_cognite_client


@pytest.fixture()
def mock_cognite_client() -> Iterable[CogniteClient]:
    """Mock CogniteClient for testing."""
    with monkeypatch_cognite_client() as m:
        yield m


@pytest.fixture
def temp_output_dir(tmp_path: Path) -> Path:
    """Temporary directory for test output."""
    output_dir = tmp_path / "udtf_output"
    output_dir.mkdir()
    return output_dir


@pytest.fixture
def sample_view() -> dm.View:
    """Sample view for testing."""
    return dm.View(
        space="test_space",
        external_id="SmallBoat",
        version="v1",
        properties={
            "name": dm.Text(),
            "description": dm.Text(),
            "boat_guid": dm.Int64(),
        },
    )


@pytest.fixture
def sample_data_model(sample_view: dm.View) -> dm.DataModel[dm.View]:
    """Sample data model for testing."""
    return dm.DataModel(
        space="test_space",
        external_id="test_model",
        version="v1",
        views=[sample_view],
    )

