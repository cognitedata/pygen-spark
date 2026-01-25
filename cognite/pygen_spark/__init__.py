"""Code generation library for creating Python UDTFs from CDF Data Models."""

from cognite.pygen_spark.config import CDFConnectionConfig

try:
    from cognite.pygen_spark.fields import UDTFField
except (
    ImportError,
    ModuleNotFoundError,
    AttributeError,
):  # pragma: no cover - fallback for environments without PySpark
    UDTFField = None  # type: ignore[assignment,misc]
from cognite.pygen_spark.generator import SparkUDTFGenerator
from cognite.pygen_spark.models import (
    UDTFGenerationResult,
    ViewSQLGenerationResult,
)

try:
    from cognite.pygen_spark.time_series_udtfs import (
        TimeSeriesDatapointsUDTF,
        TimeSeriesLatestDatapointsUDTF,
    )
except (
    ImportError,
    ModuleNotFoundError,
    AttributeError,
):  # pragma: no cover - fallback for environments without PySpark
    TimeSeriesDatapointsUDTF = None  # type: ignore[assignment,misc]
    TimeSeriesLatestDatapointsUDTF = None  # type: ignore[assignment,misc]
try:
    from cognite.pygen_spark.type_converter import TypeConverter
except (
    ImportError,
    ModuleNotFoundError,
    AttributeError,
):  # pragma: no cover - fallback for environments without PySpark
    TypeConverter = None  # type: ignore[assignment,misc]
from cognite.pygen_spark.utils import (
    InstanceId,
    parse_instance_id,
    parse_instance_ids,
    to_udtf_function_name,
)

__all__ = [
    "CDFConnectionConfig",
    "InstanceId",
    "SparkUDTFGenerator",
    "UDTFGenerationResult",
    "ViewSQLGenerationResult",
    "__version__",
    "parse_instance_id",
    "parse_instance_ids",
    "to_udtf_function_name",
]

if UDTFField is not None:
    __all__.append("UDTFField")

if TypeConverter is not None:
    __all__.append("TypeConverter")

if TimeSeriesDatapointsUDTF is not None:
    __all__.append("TimeSeriesDatapointsUDTF")

if TimeSeriesLatestDatapointsUDTF is not None:
    __all__.append("TimeSeriesLatestDatapointsUDTF")

from cognite.pygen_spark._version import __version__
