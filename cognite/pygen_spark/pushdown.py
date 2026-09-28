"""Pushdown parameters appended to every generated data model UDTF signature.

Single source of truth for the generated ``analyze()`` / ``eval()`` signatures, the catalog view SQL, and
downstream Unity Catalog function registration, which must all declare the same named arguments.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field
from pyspark.sql.types import DataType, LongType, StringType


class PushdownParameter(BaseModel):
    """A pushdown parameter accepted by generated data model UDTFs."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    name: str = Field(..., description="Parameter name in the UDTF signature")
    spark_type: DataType = Field(..., description="PySpark type used for the registered SQL signature")
    description: str = Field(..., description="Docstring text for the generated UDTF")


class PushdownParameterRegistry(BaseModel):
    """Ordered pushdown parameters, appended after the view property parameters."""

    model_config = ConfigDict(frozen=True)

    parameters: list[PushdownParameter] = Field(default_factory=list)

    @property
    def names(self) -> list[str]:
        """Parameter names in signature order."""
        return [parameter.name for parameter in self.parameters]

    @property
    def by_name(self) -> dict[str, PushdownParameter]:
        """Convenience property for dict-like access."""
        return {parameter.name: parameter for parameter in self.parameters}

    def get(self, name: str) -> PushdownParameter | None:
        """Get a pushdown parameter by name."""
        return self.by_name.get(name)


data_model_pushdown_parameters = PushdownParameterRegistry(
    parameters=[
        PushdownParameter(
            name="instance_space",
            spark_type=StringType(),
            description="Instance identity space filter (equals or JSON list)",
        ),
        PushdownParameter(
            name="external_id",
            spark_type=StringType(),
            description="Instance identity externalId filter (equals or JSON list)",
        ),
        PushdownParameter(
            name="_exists",
            spark_type=StringType(),
            description="JSON list of view properties for exists (IS NOT NULL)",
        ),
        PushdownParameter(
            name="_not_exists",
            spark_type=StringType(),
            description="JSON list of view properties for not-exists (IS NULL)",
        ),
        PushdownParameter(name="_gt", spark_type=StringType(), description="JSON map of property -> value for >"),
        PushdownParameter(name="_gte", spark_type=StringType(), description="JSON map of property -> value for >="),
        PushdownParameter(name="_lt", spark_type=StringType(), description="JSON map of property -> value for <"),
        PushdownParameter(name="_lte", spark_type=StringType(), description="JSON map of property -> value for <="),
        PushdownParameter(
            name="_row_limit",
            spark_type=LongType(),
            description="Optional row LIMIT for instances/list pagination",
        ),
        PushdownParameter(
            name="_query_mode",
            spark_type=StringType(),
            description='"list" (default) or "aggregate"',
        ),
        PushdownParameter(
            name="_aggregates",
            spark_type=StringType(),
            description="JSON list of {fn, property} for aggregate mode",
        ),
        PushdownParameter(
            name="_group_by",
            spark_type=StringType(),
            description="JSON list of groupBy property names",
        ),
    ]
)
