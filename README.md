# pygen-spark

A code generation library that extends [pygen](https://github.com/cognitedata/pygen) to generate Python User-Defined Table Functions (UDTFs) for CDF Data Models, enabling you to query CDF data directly from Spark SQL.

**Note:** This document uses PyPI package names for references:
- **PyPI:** `cognite-pygen` (repository: `pygen`)
- **PyPI:** `cognite-pygen-spark` (repository: `pygen-spark`)
- **Import paths:** `cognite.pygen`, `cognite.pygen_spark`

## Overview

`cognite.pygen_spark` (PyPI: `cognite-pygen-spark`) generates strongly-typed Python UDTF functions from CDF Data Models, allowing you to query CDF data directly from Spark SQL. The generated UDTFs work with any Spark cluster (standalone, YARN, Kubernetes, or local development).

## Features

- **UDTF Generation**: Automatically generates Python UDTF functions for each View in a CDF Data Model
- **Time Series UDTFs**: Template-generated UDTFs for querying CDF time series datapoints (single, multiple, latest) using the same template-based generation as Data Model UDTFs
- **Type Safety**: Leverages pygen's internal representation for strongly-typed code generation
- **Predicate Pushdown**: Generated UDTFs support filter translation from Spark SQL to CDF API filters
- **Configuration File Support**: Uses TOML/YAML configuration files for secure credential management
- **Generic Spark Support**: Works with any Spark cluster, not limited to Databricks
- **Type Conversion Utilities**: `TypeConverter` class for converting between CDF types, PySpark DataTypes, and SQL DDL
- **Connection Configuration**: `CDFConnectionConfig` Pydantic model for managing CDF credentials
- **Utility Functions**: Helper functions for consistent UDTF naming and other utilities

### Using Generic Spark Utilities

`pygen-spark` provides generic utilities that work with any Spark cluster:

```python
from cognite.pygen_spark import TypeConverter, CDFConnectionConfig, to_udtf_function_name

# Type conversion utilities
from cognite.client import data_modeling as dm
from pyspark.sql.types import StringType

# Convert CDF property type to PySpark DataType
spark_type = TypeConverter.cdf_to_spark(dm.Text(), is_array=False)
# Returns: StringType()

# Convert PySpark DataType to SQL DDL
sql_ddl = TypeConverter.spark_to_sql_ddl(spark_type)
# Returns: "STRING"

# Connection configuration from TOML
config = CDFConnectionConfig.from_toml("config.toml")
client = config.create_client()

# Convert view external_id to UDTF function name
udtf_name = to_udtf_function_name("MyView")
# Returns: "my_view_udtf"
```

These utilities are generic and work with any Spark cluster, not just Databricks.

## Installation

```bash
pip install cognite-pygen-spark
```

## Quick Start

```python
from pathlib import Path
from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.pygen import load_cognite_client_from_toml
from cognite.pygen_spark import SparkUDTFGenerator

# Load client from TOML file
client = load_cognite_client_from_toml("config.toml")

# Create generator
generator = SparkUDTFGenerator(
    client=client,
    output_dir=Path("./generated_udtfs"),
    data_model=DataModelId(space="sailboat", external_id="sailboat", version="1"),
    top_level_package="cognite_udtfs",
)

# Generate UDTFs for a Data Model
result = generator.generate_udtfs()

print(f"Generated {result.total_count} UDTF(s)")
for view_id, file_path in result.generated_files.items():
    print(f"  - {view_id}: {file_path}")

# Generate time series UDTFs (template-generated, same as data model UDTFs)
ts_result = generator.generate_time_series_udtfs()
print(f"Generated {ts_result.total_count} time series UDTF(s)")
for udtf_name, file_path in ts_result.generated_files.items():
    print(f"  - {udtf_name}: {file_path}")
```

See the [User Guide](docs/guide/index.md) for complete documentation on generating, registering, and querying UDTFs.

## Architecture

`cognite.pygen_spark` extends `cognite.pygen`'s architecture:

- **Reuses pygen's View parsing**: Leverages pygen's internal representation of CDF Data Models
- **Custom template engine**: Uses Jinja2 templates to generate UDTF Python code and SQL Views
- **Extends MultiAPIGenerator**: Builds on pygen's code generation infrastructure
- **Consistent template-based generation**: Both Data Model UDTFs and Time Series UDTFs use the same Jinja2 template-based generation approach for consistent behavior, error handling, and initialization patterns

See the [Technical Plan](../Technical%20Plan%20-%20CDF%20Databricks%20Integration%20(UDTF-Based).md) for detailed architecture documentation.

## Requirements

- Python 3.9+
- PySpark 3.5+ (required for UDTF support)
- `cognite-pygen` (PyPI package name; import: `cognite.pygen`)
- `cognite-sdk-python` (must be installed on all Spark worker nodes)
- Spark cluster (standalone, YARN, Kubernetes, or local)

## Package Structure

```
pygen-spark/
├── cognite/
│   └── pygen_spark/
│       ├── __init__.py
│       ├── generator.py          # SparkUDTFGenerator
│       ├── udtf_generator.py    # SparkMultiAPIGenerator
│       └── templates/
│           ├── udtf_function.py.jinja
│           ├── view_sql.py.jinja
│           └── udtf_init.py.jinja
├── pyproject.toml
└── README.md
```

## Development

### Setup

```bash
git clone <repository-url>
cd pygen-spark
pip install -e ".[dev]"
```

### Running Tests

```bash
pytest tests/
```

## Spark Cluster Compatibility

This package generates UDTF code that works with any Spark cluster:

- **Code Generation**: Works on all Spark versions ✅
- **UDTF Templates**: Compatible with PySpark 3.5+ ✅
- **Dependency Management**: Requires `cognite-sdk` on all Spark worker nodes ⚠️

For standalone Spark clusters, ensure `cognite-sdk` is installed on all worker nodes. See the [Installation Guide](docs/guide/installation.md) for details.

## Related Packages

- **[pygen](https://github.com/cognitedata/pygen)**: Base code generation library for CDF Data Models
- **[cognite-databricks](https://github.com/cognitedata/cognite-databricks)**: Helper SDK for Databricks-specific features (Unity Catalog, Secret Manager)
- **[cognite-sdk-python](https://github.com/cognitedata/cognite-sdk-python)**: Python SDK for CDF APIs

## Documentation

### User Guide

- **[Getting Started](docs/guide/index.md)**: Complete user guide for pygen-spark
- **[Installation](docs/guide/installation.md)**: Installation and setup instructions
- **[Generation](docs/guide/generation.md)**: Generate UDTF code from CDF Data Models
- **[Registration](docs/guide/registration.md)**: Register UDTFs in Spark sessions
- **[Querying](docs/guide/querying.md)**: Query UDTFs using SQL
- **[Filtering](docs/guide/filtering.md)**: Filter data with WHERE clauses
- **[Joining](docs/guide/joining.md)**: Join data from different UDTFs
- **[Troubleshooting](docs/guide/troubleshooting.md)**: Common issues and solutions

### Examples

- **[Basic Generation](examples/basic_generation.ipynb)**: Generate UDTFs from a Data Model
- **[Registration](examples/registration.ipynb)**: Register and query UDTFs
- **[Querying Data](examples/querying_data.ipynb)**: Various querying patterns
- **[Filtering Queries](examples/filtering_queries.ipynb)**: Filter examples
- **[Joining UDTFs](examples/joining_udtfs.ipynb)**: Join examples

### Technical Documentation

- [Technical Plan - CDF Databricks Integration (UDTF-Based)](../Technical%20Plan%20-%20CDF%20Databricks%20Integration%20(UDTF-Based).md)
- [Pygen Developer Documentation](https://cognite-pygen.readthedocs-hosted.com/en/latest/developer_docs/index.html)

## License

[License information]

## Contributing

[Contributing guidelines]

