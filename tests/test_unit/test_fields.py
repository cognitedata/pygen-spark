"""Unit tests for UDTFField."""

from __future__ import annotations

import pytest
from cognite.client import data_modeling as dm

from cognite.pygen_spark.fields import UDTFField

try:
    from pyspark.sql.types import StringType
except ImportError:
    pytest.skip("PySpark not available", allow_module_level=True)


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
        field = UDTFField.from_property("name", prop)
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
        field = UDTFField.from_property("boat_guid", prop)
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
        field = UDTFField.from_property("value", prop)
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
        field = UDTFField.from_property("is_active", prop)
        assert field is not None
        assert field.name == "is_active"
        assert "Boolean" in field.spark_type

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
        field = UDTFField.from_property("name", prop)
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
        field = UDTFField.from_property("related_id", prop)
        assert field is not None
        assert "String" in field.spark_type

