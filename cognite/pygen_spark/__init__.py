"""Code generation library for creating Python UDTFs from CDF Data Models."""

from cognite.pygen_spark.config import CDFConnectionConfig
from cognite.pygen_spark.fields import UDTFField
from cognite.pygen_spark.generator import SparkUDTFGenerator
from cognite.pygen_spark.models import (
    UDTFGenerationResult,
    ViewSQLGenerationResult,
)
from cognite.pygen_spark.time_series_udtfs import (
    TimeSeriesDatapointsUDTF,
    TimeSeriesDatapointsLongUDTF,
    TimeSeriesLatestDatapointsUDTF,
)
from cognite.pygen_spark.type_converter import TypeConverter
from cognite.pygen_spark.utils import (
    parse_instance_id,
    parse_instance_ids,
    to_udtf_function_name,
)

__all__ = [
    "CDFConnectionConfig",
    "SparkUDTFGenerator",
    "to_udtf_function_name",
    "parse_instance_id",
    "parse_instance_ids",
    "TypeConverter",
    "UDTFField",
    "UDTFGenerationResult",
    "ViewSQLGenerationResult",
    "TimeSeriesDatapointsUDTF",
    "TimeSeriesDatapointsLongUDTF",
    "TimeSeriesLatestDatapointsUDTF",
]

__version__ = "0.1.0"

