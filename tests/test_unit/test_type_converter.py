"""Unit tests for TypeConverter."""

from __future__ import annotations

import pytest
from cognite.client import data_modeling as dm

from cognite.pygen_spark.type_converter import TypeConverter

try:
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        DateType,
        DoubleType,
        LongType,
        StringType,
        TimestampType,
    )
except ImportError:
    pytest.skip("PySpark not available", allow_module_level=True)


class TestCdfToSpark:
    """Tests for cdf_to_spark conversion."""

    def test_int32_to_long(self) -> None:
        """Test Int32 converts to LongType."""
        result = TypeConverter.cdf_to_spark(dm.Int32())
        assert isinstance(result, LongType)

    def test_int64_to_long(self) -> None:
        """Test Int64 converts to LongType."""
        result = TypeConverter.cdf_to_spark(dm.Int64())
        assert isinstance(result, LongType)

    def test_boolean_to_boolean(self) -> None:
        """Test Boolean converts to BooleanType."""
        result = TypeConverter.cdf_to_spark(dm.Boolean())
        assert isinstance(result, BooleanType)

    def test_float32_to_double(self) -> None:
        """Test Float32 converts to DoubleType."""
        result = TypeConverter.cdf_to_spark(dm.Float32())
        assert isinstance(result, DoubleType)

    def test_float64_to_double(self) -> None:
        """Test Float64 converts to DoubleType."""
        result = TypeConverter.cdf_to_spark(dm.Float64())
        assert isinstance(result, DoubleType)

    def test_text_to_string(self) -> None:
        """Test Text converts to StringType."""
        result = TypeConverter.cdf_to_spark(dm.Text())
        assert isinstance(result, StringType)

    def test_date_to_date(self) -> None:
        """Test Date converts to DateType."""
        result = TypeConverter.cdf_to_spark(dm.Date())
        assert isinstance(result, DateType)

    def test_timestamp_to_timestamp(self) -> None:
        """Test Timestamp converts to TimestampType."""
        result = TypeConverter.cdf_to_spark(dm.Timestamp())
        assert isinstance(result, TimestampType)

    def test_direct_relation_to_string(self) -> None:
        """Test DirectRelation converts to StringType."""
        result = TypeConverter.cdf_to_spark(dm.DirectRelation())
        assert isinstance(result, StringType)

    def test_array_wrapping(self) -> None:
        """Test array wrapping."""
        result = TypeConverter.cdf_to_spark(dm.Text(), is_array=True)
        assert isinstance(result, ArrayType)
        assert isinstance(result.elementType, StringType)

    def test_unknown_type_defaults_to_string(self) -> None:
        """Test unknown type defaults to StringType."""
        # Use a mock object that's not a known type
        class UnknownType:
            pass

        result = TypeConverter.cdf_to_spark(UnknownType())
        assert isinstance(result, StringType)


class TestSparkToSqlDdl:
    """Tests for spark_to_sql_ddl conversion."""

    def test_string_type(self) -> None:
        """Test StringType converts to STRING."""
        assert TypeConverter.spark_to_sql_ddl(StringType()) == "STRING"

    def test_long_type(self) -> None:
        """Test LongType converts to INT."""
        assert TypeConverter.spark_to_sql_ddl(LongType()) == "INT"

    def test_double_type(self) -> None:
        """Test DoubleType converts to DOUBLE."""
        assert TypeConverter.spark_to_sql_ddl(DoubleType()) == "DOUBLE"

    def test_boolean_type(self) -> None:
        """Test BooleanType converts to BOOLEAN."""
        assert TypeConverter.spark_to_sql_ddl(BooleanType()) == "BOOLEAN"

    def test_date_type(self) -> None:
        """Test DateType converts to DATE."""
        assert TypeConverter.spark_to_sql_ddl(DateType()) == "DATE"

    def test_timestamp_type(self) -> None:
        """Test TimestampType converts to TIMESTAMP."""
        assert TypeConverter.spark_to_sql_ddl(TimestampType()) == "TIMESTAMP"

    def test_array_type(self) -> None:
        """Test ArrayType converts to ARRAY<...>."""
        result = TypeConverter.spark_to_sql_ddl(ArrayType(StringType()))
        assert result == "ARRAY<STRING>"

    def test_nested_array(self) -> None:
        """Test nested array type."""
        result = TypeConverter.spark_to_sql_ddl(ArrayType(ArrayType(StringType())))
        assert result == "ARRAY<ARRAY<STRING>>"


class TestSparkToSqlTypeInfo:
    """Tests for spark_to_sql_type_info conversion."""

    def test_string_type_info(self) -> None:
        """Test StringType type info."""
        sql_type, type_name = TypeConverter.spark_to_sql_type_info(StringType())
        assert sql_type == "STRING"
        # Note: type_name would be from databricks.sdk types

    def test_array_type_info(self) -> None:
        """Test ArrayType type info."""
        sql_type, type_name = TypeConverter.spark_to_sql_type_info(ArrayType(StringType()))
        assert "ARRAY" in sql_type


class TestSparkToTypeJson:
    """Tests for spark_to_type_json conversion."""

    def test_string_type_json(self) -> None:
        """Test StringType type JSON."""
        result = TypeConverter.spark_to_type_json(StringType(), "field_name", nullable=True)
        assert isinstance(result, str)
        assert "field_name" in result

    def test_array_type_json(self) -> None:
        """Test ArrayType type JSON."""
        result = TypeConverter.spark_to_type_json(ArrayType(StringType()), "field_name", nullable=True)
        assert isinstance(result, str)
        assert "field_name" in result

