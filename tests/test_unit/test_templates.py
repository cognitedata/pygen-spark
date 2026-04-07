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
        assert "CREATE" in sql.upper() or "SELECT" in sql.upper()
        # Check that all view properties are included as NULL parameters
        for prop_name in sample_view.properties.keys():
            assert f"{prop_name} => NULL" in sql or f"{prop_name}=>NULL" in sql.replace(" ", "")

    def test_reserved_property_uses_safe_name_in_udtf_and_sql(
        self,
        spark_multi_api_generator: SparkMultiAPIGenerator,
    ) -> None:
        """CDF property 'class' must map to Python/SQL identifier class_; API keys stay 'class'."""
        view = dm.View(
            space="test_space",
            external_id="ReservedWordView",
            version="v1",
            created_time=1,
            last_updated_time=2,
            name="",
            description="",
            properties={
                "class": dm.Text(),  # type: ignore[dict-item]
            },
            filter=None,
            implements=None,
            writable=False,
            used_for="all",
            is_global=False,
        )
        code = spark_multi_api_generator.generate_udtf(view, include_analyze=True, use_udtf_decorator=False)
        assert "class_:" in code
        assert 'filter_params["class"]' in code
        assert 'StructField("class_' in code
        assert '{"name": "class"' in code

        sql = spark_multi_api_generator.generate_view_sql(view=view, secret_scope="test_scope")
        assert "class_" in sql
        assert "class_ => NULL" in sql.replace(" ", "") or "class_=>NULL" in sql.replace(" ", "")
