"""Unit tests for UDTFField."""

from __future__ import annotations

import pytest
from cognite.client import data_modeling as dm

from cognite.pygen._warnings import NameCollisionViewPropertyWarning
from cognite.pygen_spark.fields import UDTFField

try:
    from pyspark.sql.types import DataType  # noqa: F401
except ImportError:
    pytest.skip("PySpark not available", allow_module_level=True)

_TEST_VIEW_ID = dm.ViewId(space="test_space", external_id="SmallBoat", version="v1")


class TestUDTFField:
    """Tests for UDTFField class."""

    def test_from_property_text(self) -> None:
        """Test creating UDTFField from Text property."""
        prop = dm.MappedProperty(
            container=dm.ContainerId("test_space", "TestContainer"),
            container_property_identifier="name",
            type=dm.Text(),
            nullable=True,
            immutable=False,
            auto_increment=False,
        )
        field = UDTFField.from_property("name", prop, _TEST_VIEW_ID)
        assert field is not None
        assert field.name == "name"
        assert field.prop_name == "name"
        assert field.nullable is True
        assert "String" in field.spark_type

    def test_from_property_int64(self) -> None:
        """Test creating UDTFField from Int64 property."""
        prop = dm.MappedProperty(
            container=dm.ContainerId("test_space", "TestContainer"),
            container_property_identifier="boat_guid",
            type=dm.Int64(),
            nullable=False,
            immutable=False,
            auto_increment=False,
        )
        field = UDTFField.from_property("boat_guid", prop, _TEST_VIEW_ID)
        assert field is not None
        assert field.name == "boat_guid"
        assert field.nullable is False
        assert "Long" in field.spark_type or "Int" in field.spark_type

    def test_from_property_float64(self) -> None:
        """Test creating UDTFField from Float64 property."""
        prop = dm.MappedProperty(
            container=dm.ContainerId("test_space", "TestContainer"),
            container_property_identifier="value",
            type=dm.Float64(),
            nullable=True,
            immutable=False,
            auto_increment=False,
        )
        field = UDTFField.from_property("value", prop, _TEST_VIEW_ID)
        assert field is not None
        assert field.name == "value"
        assert "Double" in field.spark_type

    def test_from_property_boolean(self) -> None:
        """Test creating UDTFField from Boolean property."""
        prop = dm.MappedProperty(
            container=dm.ContainerId("test_space", "TestContainer"),
            container_property_identifier="is_active",
            type=dm.Boolean(),
            nullable=True,
            immutable=False,
            auto_increment=False,
        )
        field = UDTFField.from_property("is_active", prop, _TEST_VIEW_ID)
        assert field is not None
        assert field.name == "is_active"
        assert "Boolean" in field.spark_type

    def test_from_property_timestamp_matches_typeconverter(self) -> None:
        """CDF Timestamp must map to TimestampType (str(prop.type) is JSON, not class repr)."""
        prop = dm.MappedProperty(
            container=dm.ContainerId("cdf_cdm", "CogniteSourceable"),
            container_property_identifier="sourceCreatedTime",
            type=dm.Timestamp(is_list=False, max_list_size=None),
            nullable=True,
            immutable=False,
            auto_increment=False,
        )
        field = UDTFField.from_property("sourceCreatedTime", prop, _TEST_VIEW_ID)
        assert field is not None
        assert field.spark_type == "TimestampType()"
        assert field.python_type == "datetime"
        assert field.value_kind == "timestamp"

    def test_from_property_date(self) -> None:
        """CDF Date maps to DateType via TypeConverter."""
        prop = dm.MappedProperty(
            container=dm.ContainerId("test_space", "TestContainer"),
            container_property_identifier="on_date",
            type=dm.Date(is_list=False, max_list_size=None),
            nullable=True,
            immutable=False,
            auto_increment=False,
        )
        field = UDTFField.from_property("on_date", prop, _TEST_VIEW_ID)
        assert field is not None
        assert field.spark_type == "DateType()"
        assert field.python_type == "date"
        assert field.value_kind == "date"

    def test_from_property_text_list(self) -> None:
        """List-valued Text maps to ArrayType(StringType())."""
        prop = dm.MappedProperty(
            container=dm.ContainerId("test_space", "TestContainer"),
            container_property_identifier="tags",
            type=dm.Text(is_list=True, max_list_size=1000),
            nullable=True,
            immutable=False,
            auto_increment=False,
        )
        field = UDTFField.from_property("tags", prop, _TEST_VIEW_ID)
        assert field is not None
        assert field.spark_type == "ArrayType(StringType())"
        assert field.python_type == "list[str]"
        assert field.value_kind == "array_string"

    def test_from_property_with_description(self) -> None:
        """Test creating UDTFField with description."""
        prop = dm.MappedProperty(
            container=dm.ContainerId("test_space", "TestContainer"),
            container_property_identifier="name",
            type=dm.Text(),
            nullable=True,
            immutable=False,
            auto_increment=False,
            description="Test description",
        )
        field = UDTFField.from_property("name", prop, _TEST_VIEW_ID)
        assert field is not None
        assert field.description == "Test description"

    def test_from_property_direct_relation(self) -> None:
        """Test creating UDTFField from DirectRelation property."""
        prop = dm.MappedProperty(
            container=dm.ContainerId("test_space", "TestContainer"),
            container_property_identifier="related_id",
            type=dm.DirectRelation(),
            nullable=True,
            immutable=False,
            auto_increment=False,
        )
        field = UDTFField.from_property("related_id", prop, _TEST_VIEW_ID)
        assert field is not None
        assert "String" in field.spark_type

    def test_reserved_word_class_rewrites_name(self) -> None:
        """Python keyword 'class' becomes class_ with original prop_name preserved."""
        prop = dm.MappedProperty(
            container=dm.ContainerId("test_space", "TestContainer"),
            container_property_identifier="class",
            type=dm.Text(),
            nullable=True,
            immutable=False,
            auto_increment=False,
        )
        with pytest.warns(NameCollisionViewPropertyWarning):
            field = UDTFField.from_property("class", prop, _TEST_VIEW_ID)
        assert field is not None
        assert field.prop_name == "class"
        assert field.name == "class_"
        assert field.need_alias is True

    def test_reserved_builtin_type_rewrites_name(self) -> None:
        """Builtin 'type' collides with pygen field rules and becomes type_."""
        prop = dm.MappedProperty(
            container=dm.ContainerId("test_space", "TestContainer"),
            container_property_identifier="type",
            type=dm.Text(),
            nullable=True,
            immutable=False,
            auto_increment=False,
        )
        with pytest.warns(NameCollisionViewPropertyWarning):
            field = UDTFField.from_property("type", prop, _TEST_VIEW_ID)
        assert field is not None
        assert field.prop_name == "type"
        assert field.name == "type_"
