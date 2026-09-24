"""Code generation library for creating Python UDTFs from CDF Data Models."""

from cognite.pygen_spark.config import CDFConnectionConfig
from cognite.pygen_spark.filters import (
    AggregateMetric,
    AggregateRequestSpec,
    FilterSpec,
    build_aggregate_payload,
    build_filter_json,
    effective_list_request_limit,
)

try:
    from cognite.pygen_spark.fields import UDTFField
except (
    ImportError,
    ModuleNotFoundError,
    AttributeError,
):  # pragma: no cover - fallback for environments without PySpark
    UDTFField = None  # type: ignore[assignment,misc]

try:
    from cognite.pygen_spark.generator import SparkUDTFGenerator
except (
    ImportError,
    ModuleNotFoundError,
    AttributeError,
):  # pragma: no cover - fallback for environments without PySpark
    SparkUDTFGenerator = None  # type: ignore[assignment,misc]

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
    from cognite.pygen_spark.type_converter import SparkValueKind, TypeConverter
except (
    ImportError,
    ModuleNotFoundError,
    AttributeError,
):  # pragma: no cover - fallback for environments without PySpark
    SparkValueKind = None  # type: ignore[assignment,misc]
    TypeConverter = None  # type: ignore[assignment,misc]
from cognite.pygen_spark.utils import (
    InstanceId,
    parse_instance_id,
    parse_instance_ids,
    to_udtf_function_name,
)

__all__ = [
    "AggregateMetric",
    "AggregateRequestSpec",
    "CDFConnectionConfig",
    "FilterSpec",
    "InstanceId",
    "SparkUDTFGenerator",
    "UDTFGenerationResult",
    "ViewSQLGenerationResult",
    "__version__",
    "build_aggregate_payload",
    "build_filter_json",
    "effective_list_request_limit",
    "parse_instance_id",
    "parse_instance_ids",
    "to_udtf_function_name",
]

if UDTFField is not None:
    __all__.append("UDTFField")

if TypeConverter is not None:
    __all__.append("TypeConverter")

if SparkValueKind is not None:
    __all__.append("SparkValueKind")

if TimeSeriesDatapointsUDTF is not None:
    __all__.append("TimeSeriesDatapointsUDTF")

if TimeSeriesLatestDatapointsUDTF is not None:
    __all__.append("TimeSeriesLatestDatapointsUDTF")

from cognite.pygen_spark._version import __version__
