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
from cognite.pygen_spark.utils import _check_pyspark_version

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

        Raises:
            RuntimeError: If PySpark version is less than 4.0.0
        """
        # Check PySpark version before proceeding
        _check_pyspark_version()

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

        Generates two versions of each UDTF:
        - session_scoped/: With analyze() method (for session-scoped registration)
        - catalog_registered/: Without analyze() method (for Unity Catalog registration)

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

        # Generate UDTF for each View (both versions)
        generated_files: dict[str, Path] = {}
        for view in views:
            # Generate session-scoped version (without @udtf decorator and analyze())
            # The @udtf decorator and analyze method cause PySpark to import
            # pyspark.sql.connect.udtf during serialization, which triggers a circular
            # import bug in PySpark Connect. Removing them allows registration to work.
            # The decorator will be applied during registration instead.
            udtf_code_session = self.udtf_generator.generate_udtf(
                view,
                include_analyze=False,  # Disabled to avoid circular import during serialization
                use_udtf_decorator=False,  # Disabled - will be applied during registration
            )
            file_path_session = self._write_udtf_file(view, udtf_code_session, subdirectory="session_scoped")
            generated_files[f"{view.external_id}_session"] = file_path_session

            # Generate catalog-registered version (with analyze() for UC validation)
            udtf_code_catalog = self.udtf_generator.generate_udtf(
                view,
                include_analyze=True,
                use_udtf_decorator=False,
            )
            file_path_catalog = self._write_udtf_file(view, udtf_code_catalog, subdirectory="catalog_registered")
            generated_files[f"{view.external_id}_catalog"] = file_path_catalog

        return UDTFGenerationResult(
            generated_files=generated_files,
            output_dir=self.output_dir,
            total_count=len(views),  # Count views, not files (each view generates 2 files)
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

    def _write_udtf_file(self, view: View, udtf_code: str, subdirectory: str = "") -> Path:
        """Write UDTF code to a file.

        Args:
            view: View object
            udtf_code: Generated UDTF Python code
            subdirectory: Optional subdirectory within output_dir (e.g., "session_scoped", "catalog_registered")

        Returns:
            Path to the written file
        """
        # Create output directory structure
        if subdirectory:
            udtf_dir = self.output_dir / subdirectory / self.top_level_package
        else:
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

        Generates two versions of each time series UDTF:
        - session_scoped/: With analyze() method (for session-scoped registration)
        - catalog_registered/: Without analyze() method (for Unity Catalog registration)

        Args:
            output_dir: Optional output directory. If None, uses self.output_dir.

        Returns:
            UDTFGenerationResult with generated files
        """
        if output_dir is None:
            output_dir = self.output_dir

        output_dir = Path(output_dir)

        # Map of template name to output filename
        time_series_udtfs = {
            "time_series_datapoints_udtf": "time_series_datapoints_udtf.py.jinja",
            "time_series_datapoints_detailed_udtf": "time_series_datapoints_detailed_udtf.py.jinja",
            "time_series_latest_datapoints_udtf": "time_series_latest_datapoints_udtf.py.jinja",
            "time_series_sql_udtf": "time_series_sql_udtf.py.jinja",
        }

        generated_files: dict[str, Path] = {}

        # Use the same template environment as data model UDTFs
        for file_name, template_name in time_series_udtfs.items():
            template = self.udtf_generator.env.get_template(template_name)

            # Generate session-scoped version (without @udtf decorator and analyze())
            # The @udtf decorator and analyze method cause PySpark to import
            # pyspark.sql.connect.udtf during serialization, which triggers a circular
            # import bug in PySpark Connect. Removing them allows registration to work.
            # The decorator will be applied during registration instead.
            code_session = template.render(include_analyze=False, use_udtf_decorator=False)
            # Format with Black
            try:
                import black

                code_session = black.format_str(code_session, mode=black.Mode(line_length=120))
            except (ImportError, Exception):
                pass

            udtf_dir_session = output_dir / "session_scoped" / self.top_level_package
            udtf_dir_session.mkdir(parents=True, exist_ok=True)
            file_path_session = udtf_dir_session / f"{file_name}.py"
            file_path_session.write_text(code_session, encoding="utf-8")
            generated_files[f"{file_name}_session"] = file_path_session

            # Generate catalog-registered version (with analyze() for UC validation)
            code_catalog = template.render(include_analyze=True, use_udtf_decorator=False)
            # Format with Black
            try:
                import black

                code_catalog = black.format_str(code_catalog, mode=black.Mode(line_length=120))
            except (ImportError, Exception):
                pass

            udtf_dir_catalog = output_dir / "catalog_registered" / self.top_level_package
            udtf_dir_catalog.mkdir(parents=True, exist_ok=True)
            file_path_catalog = udtf_dir_catalog / f"{file_name}.py"
            file_path_catalog.write_text(code_catalog, encoding="utf-8")
            generated_files[f"{file_name}_catalog"] = file_path_catalog

        return UDTFGenerationResult(
            generated_files=generated_files,
            output_dir=output_dir,
            total_count=len(time_series_udtfs),  # Count UDTFs, not files
        )
