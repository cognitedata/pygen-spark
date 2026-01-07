"""Tests for UDTF generation functionality."""

from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm

from cognite.pygen_spark import SparkUDTFGenerator

if TYPE_CHECKING:
    pass


def test_generate_udtfs(
    spark_udtf_generator: SparkUDTFGenerator,
    sample_view: dm.View,
) -> None:
    """Test basic UDTF generation."""
    result = spark_udtf_generator.generate_udtfs()

    assert result is not None
    assert result.total_count > 0
    assert len(result.generated_files) > 0

    # Verify files were created
    for file_path in result.file_paths:
        assert file_path.exists()
        assert file_path.suffix == ".py"

        # Verify file content
        code = file_path.read_text()
        assert len(code) > 0
        assert "class" in code


def test_generate_views(
    spark_udtf_generator: SparkUDTFGenerator,
    sample_view: dm.View,
) -> None:
    """Test SQL View generation."""
    result = spark_udtf_generator.generate_views(
        data_model=None,
        secret_scope="test_scope",
    )

    assert result is not None
    assert result.total_count > 0
    assert len(result.view_sqls) > 0

    # Verify SQL content structure
    for _view_id, sql_content in result.view_sqls.items():
        assert sql_content is not None
        assert len(sql_content) > 0
        sql_upper = sql_content.upper()
        assert "CREATE" in sql_upper or "SELECT" in sql_upper
