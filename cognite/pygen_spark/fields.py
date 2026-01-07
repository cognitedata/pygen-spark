"""UDTFField - Field representation for UDTF generation.

Similar to pygen-main's Field class, but simplified for UDTF needs.
"""

from __future__ import annotations

from dataclasses import dataclass

from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes.data_modeling.views import (
    MultiReverseDirectRelation,
    SingleReverseDirectRelation,
    ViewProperty,
)

try:
    from pyspark.sql.types import DataType
except ImportError:
    # PySpark may not be available in all environments
    DataType = object  # type: ignore[assignment, misc]


@dataclass(frozen=True)
class UDTFField:
    """Represents a property field for UDTF generation.

    Similar to pygen-main's Field class, but simplified for UDTF needs.

    Args:
        name: Property name (from view.properties key)
        prop_name: Same as name for UDTFs (no aliasing needed, kept for consistency with pygen-main)
        spark_type: Spark SQL type string (e.g., "StringType", "LongType", "ArrayType(StringType)")
        python_type: Python type annotation (e.g., "str", "int", "list[str]")
        nullable: Whether the field is nullable
        description: Optional description of the field
    """

    name: str
    prop_name: str
    spark_type: str
    python_type: str
    nullable: bool
    description: str | None = None

    @classmethod
    def from_property(
        cls,
        prop_name: str,
        prop: ViewProperty,  # MappedProperty or ConnectionDefinition
    ) -> UDTFField | None:
        """Create UDTFField from a view property.

        Similar to pygen-main's Field.from_property() pattern.

        Args:
            prop_name: The property name (key from view.properties)
            prop: The property object (MappedProperty or ConnectionDefinition)

        Returns:
            UDTFField object, or None if property should be skipped
        """
        # Get description
        description = None
        if hasattr(prop, "description") and isinstance(prop.description, str):
            # This is a workaround for the fact that the description can contain curly quotes
            # which ruff will complain about. (These come from the Core model)
            description = prop.description.replace("'", "'").replace("'", "'")

        # Determine spark_type and python_type
        spark_type = cls._get_spark_type(prop)
        python_type = cls._get_python_type(prop)

        # Determine nullable
        nullable = True
        if hasattr(prop, "nullable"):
            nullable = prop.nullable
        elif isinstance(prop, dm.MappedProperty):
            nullable = prop.nullable if hasattr(prop, "nullable") else True

        return cls(
            name=prop_name,
            prop_name=prop_name,
            spark_type=spark_type,
            python_type=python_type,
            nullable=nullable,
            description=description,
        )

    @staticmethod
    def _get_spark_type(prop: ViewProperty) -> str:
        """Convert CDF property type to Spark type instantiation code.

        Args:
            prop: Property object from view.properties

        Returns:
            Spark SQL type instantiation code (e.g., "StringType()", "LongType()", "ArrayType(StringType())")
        """

        # Helper to get base type string
        def get_base_type(prop_type: object) -> str:
            """Get base Spark type string."""
            if prop_type is None:
                return "StringType"

            type_name = str(prop_type)
            if "Text" in type_name or "str" in type_name.lower():
                return "StringType"
            elif "Int" in type_name or "int" in type_name.lower():
                return "LongType"
            elif "Float" in type_name or "float" in type_name.lower() or "Double" in type_name:
                return "DoubleType"
            elif "Bool" in type_name or "bool" in type_name.lower():
                return "BooleanType"
            elif "Timestamp" in type_name or "DateTime" in type_name:
                return "TimestampType"
            else:
                return "StringType"  # Default fallback

        # Check connection definitions first (matching pygen-main's pattern)
        if isinstance(prop, MultiReverseDirectRelation):
            return "ArrayType(StringType())"
        elif isinstance(prop, SingleReverseDirectRelation):
            return "StringType()"

        # Check if it's a MappedProperty with a DirectRelation type
        if isinstance(prop, dm.MappedProperty):
            prop_type = prop.type
            if isinstance(prop_type, dm.DirectRelation):
                # Check is_list to determine if it's multi or single (matching pygen-main's pattern)
                if prop_type.is_list if hasattr(prop_type, "is_list") else False:
                    return "ArrayType(StringType())"
                else:
                    return "StringType()"

            # Handle primitive types from MappedProperty
            # Check if it's a list type
            is_list = False
            if hasattr(prop_type, "is_list"):
                is_list = prop_type.is_list

            base_type = get_base_type(prop_type)
            if is_list:
                return f"ArrayType({base_type}())"
            else:
                return f"{base_type}()"

        # Default fallback for unknown property types
        return "StringType()"

    @staticmethod
    def _get_python_type(prop: ViewProperty) -> str:
        """Convert CDF property type to Python type annotation string.

        Uses PySpark DataType as the source of truth to ensure consistency
        with Spark types and Python types.

        Args:
            prop: Property object from view.properties

        Returns:
            Python type annotation string (e.g., "str", "int", "list[str]")
        """
        # Get the PySpark type object (which already handles is_list correctly)
        spark_type = UDTFField._get_spark_type_object(prop)

        # Convert PySpark DataType to Python type annotation
        return UDTFField._spark_type_to_python_type(spark_type)

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

        # Map base PySpark types to Python types
        if isinstance(spark_type, StringType):
            return "str"
        elif isinstance(spark_type, LongType):
            return "int"
        elif isinstance(spark_type, DoubleType):
            return "float"
        elif isinstance(spark_type, BooleanType):
            return "bool"
        elif isinstance(spark_type, TimestampType):
            return "datetime"
        elif isinstance(spark_type, DateType):
            return "date"
        else:
            # Default fallback
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
        try:
            from pyspark.sql.types import (
                ArrayType,
                BooleanType,
                DoubleType,
                LongType,
                StringType,
                TimestampType,
            )
        except ImportError:
            # If PySpark is not available, return a placeholder
            # This allows the code to work even if PySpark is not installed
            return StringType()  # type: ignore[return-value]

        # Helper to get base type object
        def get_base_type_object(prop_type: object) -> DataType:
            """Get base Spark type object."""
            if prop_type is None:
                return StringType()

            type_name = str(prop_type)
            if "Text" in type_name or "str" in type_name.lower():
                return StringType()
            elif "Int" in type_name or "int" in type_name.lower():
                return LongType()
            elif "Float" in type_name or "float" in type_name.lower() or "Double" in type_name:
                return DoubleType()
            elif "Bool" in type_name or "bool" in type_name.lower():
                return BooleanType()
            elif "Timestamp" in type_name or "DateTime" in type_name:
                return TimestampType()
            else:
                return StringType()  # Default fallback

        # Check connection definitions first (matching pygen-main's pattern)
        if isinstance(prop, MultiReverseDirectRelation):
            return ArrayType(StringType(), containsNull=True)
        elif isinstance(prop, SingleReverseDirectRelation):
            return StringType()

        # Check if it's a MappedProperty with a DirectRelation type
        if isinstance(prop, dm.MappedProperty):
            prop_type = prop.type
            if isinstance(prop_type, dm.DirectRelation):
                # Check is_list to determine if it's multi or single (matching pygen-main's pattern)
                if prop_type.is_list if hasattr(prop_type, "is_list") else False:
                    return ArrayType(StringType(), containsNull=True)
                else:
                    return StringType()

            # Handle primitive types from MappedProperty
            # Check if it's a list type
            is_list = False
            if hasattr(prop_type, "is_list"):
                is_list = prop_type.is_list

            base_type = get_base_type_object(prop_type)
            if is_list:
                return ArrayType(base_type, containsNull=True)
            else:
                return base_type

        # Default fallback for unknown property types
        return StringType()
