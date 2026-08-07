"""Unit tests for template rendering."""

from __future__ import annotations

import pytest
from cognite.client import data_modeling as dm

from cognite.pygen_spark._version import __version__ as pygen_spark_version
from cognite.pygen_spark.udtf_generator import SparkMultiAPIGenerator


def _view_with_property_count(num_props: int) -> dm.View:
    """Build a view with ``num_props`` text properties (helper for width-based tests)."""
    return dm.View(
        space="test_space",
        external_id="WideView",
        version="v1",
        created_time=1,
        last_updated_time=2,
        name="",
        description="",
        properties={f"prop{i}": dm.Text() for i in range(num_props)},  # type: ignore[dict-item]
        filter=None,
        implements=None,
        writable=False,
        used_for="all",
        is_global=False,
    )


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
        # pygen-main-style x-cdp-app: CognitePygenSpark:{version}:StandaloneUDTF:GenericSpark (defaults)
        assert "_CDF_X_CDP_APP" in code
        assert "x-cdp-sdk" in code
        assert f"CognitePygenSpark:{pygen_spark_version}:StandaloneUDTF:GenericSpark" in code

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

    def test_udtf_template_normalizes_timestamp_properties(
        self,
        spark_multi_api_generator: SparkMultiAPIGenerator,
    ) -> None:
        """Timestamp view properties must be converted to datetime for Arrow (epoch ms from API)."""
        view = dm.View(
            space="test_space",
            external_id="TsPropView",
            version="v1",
            created_time=1,
            last_updated_time=2,
            name="",
            description="",
            properties={
                "uploadedTime": dm.Timestamp(),  # type: ignore[dict-item]
            },
            filter=None,
            implements=None,
            writable=False,
            used_for="all",
            is_global=False,
        )
        code = spark_multi_api_generator.generate_udtf(view, include_analyze=True, use_udtf_decorator=False)
        assert "def _cdf_timestamp_value_to_datetime" in code
        assert "if ms == 0" not in code  # epoch 0 is a valid timestamp, not null
        assert '"value_kind"' in code
        assert 'if value_kind == "timestamp"' in code
        assert '_normalize_value(prop_value, prop["value_kind"])' in code
        assert '_cdf_timestamp_value_to_datetime(item.get("createdTime"))' in code

    def test_udtf_resolves_view_properties_once_per_instance(
        self,
        spark_multi_api_generator: SparkMultiAPIGenerator,
        sample_view: dm.View,
    ) -> None:
        """View properties are resolved once per instance, not re-walked per column."""
        code = spark_multi_api_generator.generate_udtf(sample_view, include_analyze=True, use_udtf_decorator=False)
        assert "def _resolve_view_properties" in code
        assert "view_properties = _resolve_view_properties(item)" in code
        assert "prop_value = view_properties.get(prop_name)" in code

    def test_udtf_releases_page_memory_between_pages(
        self,
        spark_multi_api_generator: SparkMultiAPIGenerator,
        sample_view: dm.View,
    ) -> None:
        """Each page's response is freed before fetching the next page."""
        code = spark_multi_api_generator.generate_udtf(sample_view, include_analyze=True, use_udtf_decorator=False)
        assert "del response, response_data, items" in code
        # nextCursor must be read before the page is released
        assert code.index('next_cursor = response_data.get("nextCursor")') < code.index(
            "del response, response_data, items"
        )

    def test_udtf_uses_adaptive_page_limit_variable(
        self,
        spark_multi_api_generator: SparkMultiAPIGenerator,
        sample_view: dm.View,
    ) -> None:
        """API request uses the computed page_limit rather than a hardcoded 1000."""
        code = spark_multi_api_generator.generate_udtf(sample_view, include_analyze=True, use_udtf_decorator=False)
        assert '"limit": page_limit' in code
        assert '"limit": 1000' not in code

    @pytest.mark.parametrize(
        ("num_props", "expected_page_limit"),
        [
            (1, 1000),
            (5, 1000),
            (50, 1000),
            (71, 704),
            (100, 500),
            (181, 276),
            (300, 166),
        ],
    )
    def test_udtf_page_size_scales_down_for_wide_views(
        self,
        spark_multi_api_generator: SparkMultiAPIGenerator,
        num_props: int,
        expected_page_limit: int,
    ) -> None:
        """Page size scales inversely with column count (min 1000, cell budget 50000)."""
        code = spark_multi_api_generator.generate_udtf(
            _view_with_property_count(num_props), include_analyze=True, use_udtf_decorator=False
        )
        assert f"page_limit = {expected_page_limit}" in code

    @pytest.mark.parametrize("num_props", [0, 1, 39, 71, 181, 300])
    def test_udtf_generated_code_is_valid_python(
        self,
        spark_multi_api_generator: SparkMultiAPIGenerator,
        num_props: int,
    ) -> None:
        """Generated UDTF code must be syntactically valid Python for any view width."""
        code = spark_multi_api_generator.generate_udtf(
            _view_with_property_count(num_props), include_analyze=True, use_udtf_decorator=False
        )
        compile(code, "<generated_udtf>", "exec")
