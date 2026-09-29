"""Parameters shared by generated UDTFs, their view SQL, and Unity Catalog registration.

Single source of truth for the generated ``analyze()`` / ``eval()`` signatures, the catalog view SQL, and
downstream Unity Catalog function registration, which must all declare the same named arguments.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field
from pyspark.sql.types import DataType, LongType, StringType


class UDTFParameter(BaseModel):
    """A parameter accepted by generated UDTFs."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    name: str = Field(..., description="Parameter name in the UDTF signature")
    spark_type: DataType = Field(..., description="PySpark type used for the registered SQL signature")
    description: str = Field(..., description="Docstring text for the generated UDTF")


class UDTFParameterRegistry(BaseModel):
    """Ordered UDTF parameters."""

    model_config = ConfigDict(frozen=True)

    parameters: list[UDTFParameter] = Field(default_factory=list)

    @property
    def names(self) -> list[str]:
        """Parameter names in signature order."""
        return [parameter.name for parameter in self.parameters]

    @property
    def by_name(self) -> dict[str, UDTFParameter]:
        """Convenience property for dict-like access."""
        return {parameter.name: parameter for parameter in self.parameters}

    def get(self, name: str) -> UDTFParameter | None:
        """Get a parameter by name."""
        return self.by_name.get(name)


# Appended after the view property parameters of every data model UDTF.
data_model_pushdown_parameters = UDTFParameterRegistry(
    parameters=[
        UDTFParameter(
            name="instance_space",
            spark_type=StringType(),
            description="Instance identity space filter (equals or JSON list)",
        ),
        UDTFParameter(
            name="external_id",
            spark_type=StringType(),
            description="Instance identity externalId filter (equals or JSON list)",
        ),
        UDTFParameter(
            name="_exists",
            spark_type=StringType(),
            description="JSON list of view properties for exists (IS NOT NULL)",
        ),
        UDTFParameter(
            name="_not_exists",
            spark_type=StringType(),
            description="JSON list of view properties for not-exists (IS NULL)",
        ),
        UDTFParameter(name="_gt", spark_type=StringType(), description="JSON map of property -> value for >"),
        UDTFParameter(name="_gte", spark_type=StringType(), description="JSON map of property -> value for >="),
        UDTFParameter(name="_lt", spark_type=StringType(), description="JSON map of property -> value for <"),
        UDTFParameter(name="_lte", spark_type=StringType(), description="JSON map of property -> value for <="),
        UDTFParameter(
            name="_row_limit",
            spark_type=LongType(),
            description="Optional row LIMIT for instances/list pagination",
        ),
        UDTFParameter(name="_query_mode", spark_type=StringType(), description='"list" (default) or "aggregate"'),
        UDTFParameter(
            name="_aggregates",
            spark_type=StringType(),
            description="JSON list of {fn, property} for aggregate mode",
        ),
        UDTFParameter(name="_group_by", spark_type=StringType(), description="JSON list of groupBy property names"),
    ]
)

# Last parameter of every generated UDTF (data model and time series). Stored in Secret Manager as ``base_url``.
base_url_parameter = UDTFParameter(
    name="base_url",
    spark_type=StringType(),
    description="CDF API base URL (Private Link / dedicated); empty uses https://{cdf_cluster}.cognitedata.com",
)
