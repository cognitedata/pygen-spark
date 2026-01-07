"""Test utilities for pygen-spark tests."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cognite.client.data_classes.data_modeling import View


def assert_udtf_file_valid(file_path: Path) -> None:
    """Assert that a UDTF file is valid.

    Args:
        file_path: Path to the UDTF file

    Raises:
        AssertionError: If file is invalid
    """
    assert file_path.exists(), f"UDTF file does not exist: {file_path}"
    assert file_path.suffix == ".py", f"UDTF file should be .py: {file_path}"

    code = file_path.read_text()
    assert len(code) > 0, f"UDTF file is empty: {file_path}"
    assert "class" in code, f"UDTF file should contain class definition: {file_path}"


def assert_sql_view_valid(sql_content: str, view_id: str) -> None:
    """Assert that a SQL view is valid.

    Args:
        sql_content: SQL view content
        view_id: Expected view ID

    Raises:
        AssertionError: If SQL is invalid
    """
    assert sql_content is not None, "SQL content should not be None"
    assert len(sql_content) > 0, "SQL content should not be empty"
    sql_upper = sql_content.upper()
    assert "CREATE" in sql_upper or "SELECT" in sql_upper, "SQL should contain CREATE or SELECT"


def create_mock_view(
    space: str = "test_space",
    external_id: str = "TestView",
    version: str = "v1",
    properties: dict | None = None,
) -> View:
    """Create a mock view for testing.

    Args:
        space: View space
        external_id: View external ID
        version: View version
        properties: View properties (defaults to simple text property)

    Returns:
        Mock View object
    """
    from cognite.client import data_modeling as dm

    if properties is None:
        properties = {"name": dm.Text()}

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    return dm.View(
        space=space,
        external_id=external_id,
        version=version,
        properties=properties,
        created_time=now_ms,
        last_updated_time=now_ms,
        name=external_id,
        description="",
        filter=None,
        implements=[],
        writable=False,
        used_for="node",
        is_global=False,
    )
