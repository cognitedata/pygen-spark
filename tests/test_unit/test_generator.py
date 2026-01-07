"""Unit tests for SparkUDTFGenerator."""

from __future__ import annotations

from pathlib import Path

import pytest
from cognite.client import data_modeling as dm

from cognite.pygen_spark import SparkUDTFGenerator


class TestSparkUDTFGenerator:
    """Tests for SparkUDTFGenerator."""

    def test_init(
        self,
        mock_cognite_client,
        temp_output_dir: Path,
        sample_data_model: dm.DataModel[dm.View],
    ) -> None:
        """Test generator initialization."""
        generator = SparkUDTFGenerator(
            client=mock_cognite_client,
            output_dir=temp_output_dir,
            data_model=sample_data_model,
        )
        assert generator.output_dir == temp_output_dir
        assert generator.client == mock_cognite_client

    def test_generate_udtfs(
        self,
        spark_udtf_generator: SparkUDTFGenerator,
        sample_view: dm.View,
    ) -> None:
        """Test UDTF generation."""
        result = spark_udtf_generator.generate_udtfs()
        assert result is not None
        assert result.total_count > 0
        assert len(result.generated_files) > 0

    def test_generate_udtfs_creates_files(
        self,
        spark_udtf_generator: SparkUDTFGenerator,
        temp_output_dir: Path,
    ) -> None:
        """Test that UDTF generation creates files."""
        result = spark_udtf_generator.generate_udtfs()
        for file_path in result.file_paths:
            assert file_path.exists()
            assert file_path.suffix == ".py"

    def test_generate_views(
        self,
        spark_udtf_generator: SparkUDTFGenerator,
        sample_view: dm.View,
    ) -> None:
        """Test SQL view generation."""
        result = spark_udtf_generator.generate_views(
            data_model=None,
            secret_scope="test_scope",
        )
        assert result is not None
        assert result.total_count > 0
        assert len(result.view_sqls) > 0
        
        # Verify SQL content structure
        for view_id, sql_content in result.view_sqls.items():
            assert sql_content is not None
            sql_upper = sql_content.upper()
            assert "CREATE" in sql_upper or "SELECT" in sql_upper

