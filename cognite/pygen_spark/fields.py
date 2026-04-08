"""UDTFField - Field representation for UDTF generation.

Similar to pygen-main's Field class, but simplified for UDTF needs.
CDF ``PropertyType`` values are mapped via :class:`~cognite.pygen_spark.type_converter.TypeConverter`
(``isinstance`` / PySpark DataTypes), not ``str(prop.type)`` — Cognite types stringify as JSON dumps.
"""

from __future__ import annotations

from dataclasses import dataclass

from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes.data_modeling.views import (
    MultiReverseDirectRelation,
    SingleReverseDirectRelation,
    ViewProperty,
)

from cognite.pygen.config.reserved_words import is_reserved_word
from cognite.pygen_spark.type_converter import SparkValueKind, TypeConverter

try:
    from pyspark.sql.types import DataType
except (ImportError, ModuleNotFoundError, AttributeError):
    # PySpark may not be available or may fail on some platforms
    DataType = object  # type: ignore[assignment, misc]


@dataclass(frozen=True)
class UDTFField:
    """Represents a property field for UDTF generation.

    Similar to pygen-main's Field class, but simplified for UDTF needs.

    Args:
        name: Safe Python / Spark identifier for generated UDTF parameters and StructField names.
        prop_name: CDF view property key (used in API filters and response parsing).
        spark_type: PySpark type as a constructor expression (e.g. ``StringType()``, from TypeConverter)
        python_type: Python type annotation (e.g., "str", "int", "list[str]")
        nullable: Whether the field is nullable
        description: Optional description of the field
        is_array: Whether the property is an array type in the View definition
        value_kind: Normalization discriminator derived from PySpark ``DataType`` (``SparkValueKind.value``)
    """

    name: str
    prop_name: str
    spark_type: str
    python_type: str
    nullable: bool
    description: str | None = None
    is_array: bool = False
    value_kind: str = SparkValueKind.STRING.value

    @property
    def need_alias(self) -> bool:
        """True when generated Python/SQL name differs from the CDF property id."""
        return self.name != self.prop_name

    @classmethod
    def from_property(
        cls,
        prop_name: str,
        prop: ViewProperty,  # MappedProperty or ConnectionDefinition
        view_id: dm.ViewId,
    ) -> UDTFField | None:
        """Create UDTFField from a view property.

        Similar to pygen-main's Field.from_property() pattern: reserved Python words get a trailing
        underscore in ``name`` while ``prop_name`` stays the CDF key.

        Args:
            prop_name: The property name (key from view.properties)
            prop: The property object (MappedProperty or ConnectionDefinition)
            view_id: View identifier for NameCollisionWarning context (use ``view.as_id()``)

        Returns:
            UDTFField object, or None if property should be skipped
        """
        # Get description
        description = None
        if hasattr(prop, "description") and isinstance(prop.description, str):
            # This is a workaround for the fact that the description can contain curly quotes
            # which ruff will complain about. (These come from the Core model)
            description = prop.description.replace("'", "'").replace("'", "'")

        spark_dt = cls._get_spark_type_object(prop)
        spark_type = TypeConverter.spark_to_type_instantiation_code(spark_dt)
        value_kind = TypeConverter.spark_value_kind(spark_dt).value
        python_type = cls._spark_type_to_python_type(spark_dt)

        # Determine nullable
        nullable = True
        if hasattr(prop, "nullable"):
            nullable = prop.nullable
        elif isinstance(prop, dm.MappedProperty):
            nullable = prop.nullable if hasattr(prop, "nullable") else True

        # Determine if property is an array type
        is_array = False
        if isinstance(prop, dm.MappedProperty):
            prop_type = prop.type
            if hasattr(prop_type, "is_list"):
                is_array = prop_type.is_list

        name = prop_name
        if is_reserved_word(name, "field", view_id, prop_name):
            name = f"{name}_"

        return cls(
            name=name,
            prop_name=prop_name,
            spark_type=spark_type,
            python_type=python_type,
            nullable=nullable,
            description=description,
            is_array=is_array,
            value_kind=value_kind,
        )

    @staticmethod
    def _get_spark_type(prop: ViewProperty) -> str:
        """Return Spark type instantiation code string (same object graph as :meth:`_get_spark_type_object`)."""
        return TypeConverter.spark_to_type_instantiation_code(UDTFField._get_spark_type_object(prop))

    @staticmethod
    def _spark_type_to_python_type(spark_type: DataType) -> str:
        """Convert PySpark DataType to Python type annotation string.

        Args:
            spark_type: PySpark DataType object (e.g., StringType(), ArrayType(StringType()))

        Returns:
            Python type annotation string (e.g., "str", "int", "list[str]")
        """
        try:
            from pyspark.sql.types import (
                ArrayType,
                BooleanType,
                DateType,
                DoubleType,
                IntegerType,
                LongType,
                StringType,
                TimestampType,
            )
        except ImportError:
            # If PySpark is not available, return default
            return "str"

        # Handle ArrayType - extract element type and wrap in list[...]
        if isinstance(spark_type, ArrayType):
            element_type = spark_type.elementType
            element_python_type = UDTFField._spark_type_to_python_type(element_type)
            return f"list[{element_python_type}]"

        # Map base PySpark types to Python types (isinstance-only, aligned with TypeConverter)
        if isinstance(spark_type, StringType):
            return "str"
        if isinstance(spark_type, (LongType, IntegerType)):
            return "int"
        if isinstance(spark_type, DoubleType):
            return "float"
        if isinstance(spark_type, BooleanType):
            return "bool"
        if isinstance(spark_type, TimestampType):
            return "datetime"
        if isinstance(spark_type, DateType):
            return "date"
        return "str"

    @staticmethod
    def _get_spark_type_object(prop: ViewProperty) -> DataType:
        """Convert CDF property type to actual PySpark DataType object.

        This is useful for validation and comparison, while _get_spark_type()
        returns strings for code generation.

        Args:
            prop: Property object from view.properties

        Returns:
            PySpark DataType object (e.g., StringType(), ArrayType(StringType()))
        """
        from pyspark.sql.types import StringType

        # Check connection definitions first (matching pygen-main's pattern)
        if isinstance(prop, MultiReverseDirectRelation):
            # Represent multi relations as JSON strings to keep UC registration compatible.
            return StringType()
        if isinstance(prop, SingleReverseDirectRelation):
            return StringType()

        if isinstance(prop, dm.MappedProperty):
            prop_type = prop.type
            if isinstance(prop_type, dm.DirectRelation):
                if prop_type.is_list if hasattr(prop_type, "is_list") else False:
                    return StringType()
                return StringType()

            is_list = bool(getattr(prop_type, "is_list", False))
            return TypeConverter.cdf_to_spark(prop_type, is_array=is_list)

        return StringType()
