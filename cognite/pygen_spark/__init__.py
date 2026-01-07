"""Code generation library for creating Python UDTFs from CDF Data Models."""

from cognite.pygen_spark.config import CDFConnectionConfig
from cognite.pygen_spark.fields import UDTFField
from cognite.pygen_spark.generator import SparkUDTFGenerator
from cognite.pygen_spark.models import (
    UDTFGenerationResult,
    ViewSQLGenerationResult,
)
from cognite.pygen_spark.time_series_udtfs import (
    TimeSeriesDatapointsLongUDTF,
    TimeSeriesDatapointsUDTF,
    TimeSeriesLatestDatapointsUDTF,
)
from cognite.pygen_spark.type_converter import TypeConverter
from cognite.pygen_spark.utils import (
    InstanceId,
    parse_instance_id,
    parse_instance_ids,
    to_udtf_function_name,
)

__all__ = [
    "__version__",
    "CDFConnectionConfig",
    "InstanceId",
    "SparkUDTFGenerator",
    "TimeSeriesDatapointsLongUDTF",
    "TimeSeriesDatapointsUDTF",
    "TimeSeriesLatestDatapointsUDTF",
    "TypeConverter",
    "UDTFField",
    "UDTFGenerationResult",
    "ViewSQLGenerationResult",
    "parse_instance_id",
    "parse_instance_ids",
    "to_udtf_function_name",
]

from cognite.pygen_spark._version import __version__
