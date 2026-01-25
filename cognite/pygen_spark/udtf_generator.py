"""SparkMultiAPIGenerator - Extends pygen's MultiAPIGenerator for UDTF generation."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from jinja2 import Environment, PackageLoader, select_autoescape

# This pattern applies to ALL pygen dependencies, not just the ones listed here
# Import relationship types directly from views module (matching pygen-main's approach)
# Short-term: Import from private API (required until pygen exports this)
# This pattern applies to ALL pygen dependencies, not just the ones listed here
from cognite.pygen._core.generators import MultiAPIGenerator  # type: ignore[import-untyped]
from cognite.pygen_spark.fields import UDTFField

if TYPE_CHECKING:
    from cognite.client.data_classes.data_modeling import View


class SparkMultiAPIGenerator(MultiAPIGenerator):
    """Extends pygen's MultiAPIGenerator to generate UDTF code instead of Pydantic models.

    Uses pygen's internal representation of the View (properties, types, etc.)
    but generates Python UDTF functions instead of Pydantic models.
    """

    def __init__(self, *args: object, **kwargs: dict[str, object]) -> None:
        """Initialize the Spark Multi-API generator.

        Args:
            *args: Arguments passed to parent MultiAPIGenerator
            **kwargs: Keyword arguments passed to parent MultiAPIGenerator
        """
        super().__init__(*args, **kwargs)
        # Override template environment to use UDTF templates from cognite.pygen_spark.templates
        # MultiAPIGenerator uses self.env for template loading, not self.template_loader
        # Match pygen-main's environment configuration (no Undefined)
        # Note: We disable trim_blocks and lstrip_blocks to preserve formatting in generated code
        self.env = Environment(
            loader=PackageLoader("cognite.pygen_spark", "templates"),
            autoescape=select_autoescape(),
            trim_blocks=False,
            lstrip_blocks=False,
        )

        # Add custom filter to escape Python strings for use in docstrings and string literals
        def escape_python_string(value: str | None) -> str:
            """Escape a string for safe use in Python docstrings.

            Handles quotes, newlines, and other special characters that could break
            Python string literals. For docstrings (triple-quoted), we replace problematic
            characters with safe alternatives.
            """
            if value is None:
                return ""
            # For docstrings (triple-quoted), we need to handle triple quotes specially
            # Replace triple quotes with escaped version to prevent breaking docstring
            escaped = value.replace('"""', '\\"\\"\\"')
            # Also handle single and double quotes that might appear
            # Replace newlines with spaces (docstrings should be single-line for parameter descriptions)
            escaped = escaped.replace("\n", " ").replace("\r", " ")
            # Replace tabs with spaces
            escaped = escaped.replace("\t", " ")
            # Collapse multiple spaces
            escaped = re.sub(r"\s+", " ", escaped)
            return escaped.strip()

        # Add filter to escape strings for use in Python string literals (with quotes)
        def escape_python_literal(value: str | None) -> str:
            """Escape a string for safe use in Python string literals (with quotes).

            Properly escapes quotes, backslashes, and other special characters.
            """
            if value is None:
                return ""
            # Escape backslashes first, then quotes
            escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("'", "\\'")
            # Replace newlines with \n escape sequence
            escaped = escaped.replace("\n", "\\n").replace("\r", "\\r")
            # Replace tabs with \t escape sequence
            escaped = escaped.replace("\t", "\\t")
            return escaped

        self.env.filters["escape_python_string"] = escape_python_string
        self.env.filters["escape_python_literal"] = escape_python_literal
        # Note: We don't need to store data_model anymore - views are independent
        # and we use view.as_id() directly in the template

    def generate_udtf(
        self,
        view: View,
        include_analyze: bool = True,
        use_udtf_decorator: bool = True,
    ) -> str:
        """Generate Python UDTF code for a View.

        Args:
            view: View object from CDF Data Model
            include_analyze: If True, includes analyze() method (for session-scoped).
                            If False, omits analyze() (for Unity Catalog).
            use_udtf_decorator: If True, adds @udtf decorator to the class.

        Returns:
            Generated UDTF Python code as string (formatted with Black)
        """
        # Load UDTF template using self.env (inherited from parent, but overridden to use our templates)
        template = self.env.get_template("udtf_function.py.jinja")

        # Create UDTFField objects from view properties (matching pygen-main's Field.from_property pattern)
        udtf_fields = []
        for prop_name, prop in view.properties.items():
            udtf_field = UDTFField.from_property(prop_name, prop)
            if udtf_field is not None:
                udtf_fields.append(udtf_field)

        # Render template with view and UDTFField objects (matching pygen-main's pattern)
        # Note: Views are independent of data models - we use view.as_id() directly in the template
        code = template.render(
            view=view,
            properties=udtf_fields,  # Pass UDTFField objects (like pygen-main passes Field objects)
            include_analyze=include_analyze,  # Pass include_analyze to template
            use_udtf_decorator=use_udtf_decorator,
        )

        # Format the generated code using Black (like pygen-main does)
        # This ensures proper formatting and fixes issues with collapsed whitespace
        try:
            import black

            code = black.format_str(code, mode=black.Mode(line_length=120))
        except ImportError:
            # If black is not available, just return the code as-is
            # This allows the code to work even if black is not installed
            pass
        except (ValueError, SyntaxError, TypeError):
            # If formatting fails for any reason, return the code as-is
            # This prevents formatting errors from breaking code generation
            pass

        return code

    def generate_view_sql(
        self, view: View, secret_scope: str, catalog: str | None = None, schema: str | None = None
    ) -> str:
        """Generate SQL CREATE VIEW statement with Secret injection.

        Args:
            view: View object from CDF Data Model
            secret_scope: Databricks Secret Manager scope name
                The secrets referenced (client_id, client_secret, tenant_id, cdf_cluster, project)
                come from the TOML file and are stored in Secret Manager.
            catalog: Optional catalog name. If None, uses placeholder "{{ catalog }}"
            schema: Optional schema name. If None, uses placeholder "{{ schema }}"

        Returns:
            SQL CREATE VIEW statement
        """
        from cognite.pygen_spark.utils import to_udtf_function_name

        template = self.env.get_template("view_sql.py.jinja")

        # Pass catalog and schema to template, or use placeholders if not provided
        # If None, template will use the placeholder strings directly
        template_vars = {
            "view": view,
            "secret_scope": secret_scope,
            "udtf_name": to_udtf_function_name(view.external_id),  # Use consistent snake_case conversion
        }

        # Only add catalog/schema if provided (otherwise template will use placeholders)
        if catalog is not None:
            template_vars["catalog"] = catalog
        if schema is not None:
            template_vars["schema"] = schema

        return template.render(**template_vars)
