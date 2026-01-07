"""Unit tests for template rendering."""

from __future__ import annotations

from cognite.client import data_modeling as dm

from cognite.pygen_spark.udtf_generator import SparkMultiAPIGenerator


class TestTemplateRendering:
    """Tests for Jinja template rendering."""

    def test_udtf_template_renders(
        self,
        spark_multi_api_generator: SparkMultiAPIGenerator,
        sample_view: dm.View,
    ) -> None:
        """Test that UDTF template renders successfully."""
        code = spark_multi_api_generator.generate_udtf(sample_view)
        assert code is not None
        assert isinstance(code, str)
        assert len(code) > 0
        # Check for key UDTF components
        assert "class" in code.lower()
        assert "eval" in code.lower() or "__call__" in code.lower()

    def test_udtf_template_includes_view_properties(
        self,
        spark_multi_api_generator: SparkMultiAPIGenerator,
        sample_view: dm.View,
    ) -> None:
        """Test that generated UDTF includes view properties."""
        code = spark_multi_api_generator.generate_udtf(sample_view)
        # Check that property names appear in the code
        for prop_name in sample_view.properties.keys():
            assert prop_name in code or prop_name.lower() in code.lower()

    def test_view_sql_template_renders(
        self,
        spark_multi_api_generator: SparkMultiAPIGenerator,
        sample_view: dm.View,
    ) -> None:
        """Test that SQL view template renders successfully."""
        sql = spark_multi_api_generator.generate_view_sql(
            view=sample_view,
            secret_scope="test_scope",
        )
        assert sql is not None
        assert isinstance(sql, str)
        assert len(sql) > 0
        # Check for SQL keywords
        assert "SELECT" in sql.upper() or "CREATE" in sql.upper()
