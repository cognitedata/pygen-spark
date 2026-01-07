"""SparkUDTFGenerator - Main generator class for UDTF code generation."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling import DataModelIdentifier

# Short-term: Import from private API (required until pygen exports these)
# This pattern applies to ALL pygen dependencies, not just the ones listed here
from cognite.pygen._core.generators import SDKGenerator  # type: ignore[import-untyped]
from cognite.pygen.config import PygenConfig  # type: ignore[import-untyped]
from cognite.pygen_spark.models import (
    UDTFGenerationResult,
    ViewSQLGenerationResult,
)
from cognite.pygen_spark.udtf_generator import SparkMultiAPIGenerator

if TYPE_CHECKING:
    from cognite.client.data_classes.data_modeling import View

# Define DataModel type alias (same as pygen)
# Short-term: We define our own type alias using public types from cognite.client
# This avoids depending on pygen's private API (_generator module)
# This pattern applies to ALL pygen dependencies, not just the ones listed here
DataModel = DataModelIdentifier | dm.DataModel[dm.View]


class SparkUDTFGenerator(SDKGenerator):
    """Generator for creating Python UDTF functions from CDF Data Models.

    Extends pygen's SDKGenerator to reuse View parsing logic.
    """

    def __init__(
        self,
        client: CogniteClient,
        output_dir: Path,
        data_model: DataModel,
        top_level_package: str = "cognite_databricks",
        client_name: str = "CogniteDatabricksClient",
        **kwargs: dict[str, object],
    ) -> None:
        """Initialize the Spark UDTF generator.

        Args:
            client: CogniteClient instance
            output_dir: Directory where generated UDTF files will be written
            data_model: DataModel identifier (DataModelId or DataModel object)
            top_level_package: Top-level Python package name for generated code
            client_name: Name of the client class (required by parent SDKGenerator)
            **kwargs: Additional arguments passed to parent SDKGenerator
        """
        # Load data model if it's an identifier
        loaded_data_model = self._load_data_model(data_model, client)

        # Call parent with correct signature: top_level_package, client_name, data_model, ...
        super().__init__(
            top_level_package=top_level_package,
            client_name=client_name,
            data_model=loaded_data_model,
            **kwargs,
        )

        # Store client and output_dir for later use
        self.client = client
        self.output_dir = output_dir

        # Create SparkMultiAPIGenerator with correct parameters matching MultiAPIGenerator signature
        # MultiAPIGenerator.__init__ requires:
        #   top_level_package, client_name, data_models, default_instance_space, implements, logger, config
        data_models_list = (
            [loaded_data_model] if isinstance(loaded_data_model, dm.DataModel) else list(loaded_data_model)
        )
        self.udtf_generator = SparkMultiAPIGenerator(  # type: ignore
            top_level_package=top_level_package,  # type: ignore[arg-type]
            client_name=client_name,  # type: ignore[arg-type]
            data_models=data_models_list,  # type: ignore[arg-type]
            default_instance_space=kwargs.get("default_instance_space", None),  # type: ignore[arg-type]
            implements=kwargs.get("implements", "inheritance"),  # type: ignore[arg-type]
            logger=kwargs.get("logger", None),  # type: ignore[arg-type]
            config=kwargs.get("config", PygenConfig()),  # type: ignore[arg-type]
        )

    def _load_data_model(self, data_model: DataModel, client: CogniteClient) -> dm.DataModel[dm.View]:
        """Load data model from identifier if needed.

        Args:
            data_model: DataModel identifier or DataModel object
            client: CogniteClient instance

        Returns:
            Loaded DataModel object
        """
        if isinstance(data_model, dm.DataModel):
            return data_model

        # Load data model from CDF using the client
        # Use the same pattern as pygen's _get_data_model
        data_models = client.data_modeling.data_models.retrieve(data_model, inline_views=True)
        if not data_models:
            raise ValueError(f"Data Model not found: {data_model}")

        # Return the first (and only) data model
        if len(data_models) == 1:
            return data_models[0]
        else:
            raise ValueError(f"Expected single data model, got {len(data_models)}")

    def generate_udtfs(self, data_model: DataModel | None = None) -> UDTFGenerationResult:
        """Generate UDTF functions for all Views in a Data Model.

        Args:
            data_model: Optional DataModel identifier. If None, uses the data model from __init__.

        Returns:
            UDTFGenerationResult with structured information about generated files.
            Access individual files via result['view_id'] or result.get_file('view_id').
        """
        # Use the data model from __init__ if not provided
        if data_model is None:
            data_model_obj = self._data_model[0] if isinstance(self._data_model, list) else self._data_model
        else:
            data_model_obj = self._load_data_model(data_model, self.client)

        # Reuse pygen's View parsing (same pattern as pygen)
        views = self._load_views(data_model_obj)

        # Generate UDTF for each View
        generated_files: dict[str, Path] = {}
        for view in views:
            udtf_code = self.udtf_generator.generate_udtf(view)
            file_path = self._write_udtf_file(view, udtf_code)
            generated_files[view.external_id] = file_path

        return UDTFGenerationResult(
            generated_files=generated_files,
            output_dir=self.output_dir,
            total_count=len(generated_files),
        )

    def generate_views(
        self,
        data_model: DataModel | None = None,
        secret_scope: str = "",
        catalog: str | None = None,
        schema: str | None = None,
    ) -> ViewSQLGenerationResult:
        """Generate SQL View definitions with Secret injection.

        Args:
            data_model: Optional DataModel identifier. If None, uses the data model from __init__.
            secret_scope: Databricks Secret Manager scope name
            catalog: Optional catalog name. If None, uses placeholder "{{ catalog }}"
            schema: Optional schema name. If None, uses placeholder "{{ schema }}"

        Returns:
            ViewSQLGenerationResult with structured information about generated SQL statements.
            Access individual SQL via result['view_id'] or result.get_sql('view_id').
        """
        # Use the data model from __init__ if not provided
        if data_model is None:
            data_model_obj = self._data_model[0] if isinstance(self._data_model, list) else self._data_model
        else:
            data_model_obj = self._load_data_model(data_model, self.client)

        views = self._load_views(data_model_obj)

        # Generate View SQL for each View
        view_sqls: dict[str, str] = {}
        for view in views:
            view_sql = self.udtf_generator.generate_view_sql(view, secret_scope, catalog=catalog, schema=schema)
            view_sqls[view.external_id] = view_sql

        return ViewSQLGenerationResult(
            view_sqls=view_sqls,
            total_count=len(view_sqls),
        )

    def _load_views(self, data_model: dm.DataModel[dm.View]) -> list[View]:
        """Load views from data model.

        Args:
            data_model: DataModel object

        Returns:
            List of View objects
        """
        # data_model.views is a list, not a dict
        return list(data_model.views)

    def _write_udtf_file(self, view: View, udtf_code: str) -> Path:
        """Write UDTF code to a file.

        Args:
            view: View object
            udtf_code: Generated UDTF Python code

        Returns:
            Path to the written file
        """
        # Create output directory structure
        udtf_dir = self.output_dir / self.top_level_package
        udtf_dir.mkdir(parents=True, exist_ok=True)

        # Write UDTF file
        file_path = udtf_dir / f"{view.external_id}_udtf.py"
        file_path.write_text(udtf_code, encoding="utf-8")

        return file_path  # type: ignore[return-value]

    def generate_time_series_udtfs(
        self,
        output_dir: Path | None = None,
    ) -> UDTFGenerationResult:
        """Generate time series UDTF files using templates.

        Generates the three standard time series UDTFs:
        - time_series_datapoints_udtf
        - time_series_datapoints_long_udtf
        - time_series_latest_datapoints_udtf

        Uses the same template-based generation pattern as data model UDTFs for consistency.

        Args:
            output_dir: Optional output directory. If None, uses self.output_dir.

        Returns:
            UDTFGenerationResult with generated files
        """
        if output_dir is None:
            output_dir = self.output_dir

        output_dir = Path(output_dir)
        udtf_dir = output_dir / self.top_level_package
        udtf_dir.mkdir(parents=True, exist_ok=True)

        # Map of template name to output filename
        time_series_udtfs = {
            "time_series_datapoints_udtf": "time_series_datapoints_udtf.py.jinja",
            "time_series_datapoints_long_udtf": "time_series_datapoints_long_udtf.py.jinja",
            "time_series_latest_datapoints_udtf": "time_series_latest_datapoints_udtf.py.jinja",
        }

        generated_files: dict[str, Path] = {}

        # Use the same template environment as data model UDTFs
        for file_name, template_name in time_series_udtfs.items():
            # Load template using the same environment as data model UDTFs
            template = self.udtf_generator.env.get_template(template_name)

            # Render template (no variables needed for time series UDTFs)
            udtf_code = template.render()

            # Format with Black if available (same as data model UDTFs)
            try:
                import black

                udtf_code = black.format_str(udtf_code, mode=black.Mode(line_length=120))
            except ImportError:
                pass
            except Exception:
                pass

            # Write to file using the same pattern as data model UDTFs
            file_path = udtf_dir / f"{file_name}.py"
            file_path.write_text(udtf_code, encoding="utf-8")
            generated_files[file_name] = file_path

        return UDTFGenerationResult(
            generated_files=generated_files,
            output_dir=output_dir,
            total_count=len(generated_files),
        )
