# pygen-spark

A code generation library that extends [pygen](https://github.com/cognitedata/pygen) to generate Python User-Defined Table Functions (UDTFs) for CDF Data Models, enabling native Unity Catalog governance in Databricks.

**Note:** This document uses PyPI package names for references:
- **PyPI:** `cognite-pygen` (repository: `pygen`)
- **PyPI:** `cognite-pygen-spark` (repository: `pygen-spark`)
- **Import paths:** `cognite.pygen`, `cognite.pygen_spark`

## Overview

`cognite.pygen_spark` (PyPI: `cognite-pygen-spark`) generates strongly-typed Python UDTF functions from CDF Data Models, allowing you to query CDF data directly from Databricks SQL with full Unity Catalog integration. The generated UDTFs can be registered in Unity Catalog and exposed as discoverable Views.

## Features

- **UDTF Generation**: Automatically generates Python UDTF functions for each View in a CDF Data Model
- **SQL View Generation**: Creates Unity Catalog View definitions with Secret Manager integration
- **Type Safety**: Leverages pygen's internal representation for strongly-typed code generation
- **Predicate Pushdown**: Generated UDTFs support filter translation from Spark SQL to CDF API filters
- **Secret Management**: Integrates with Databricks Secret Manager for secure credential handling

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
    top_level_package="cognite_databricks",
)

# Generate UDTFs for a Data Model
data_model_id = DataModelId(space="sp_pygen_power", external_id="WindTurbine", version="1")
udtf_files = generator.generate_udtfs(data_model_id)

# Generate View SQL with Secret injection
secret_scope = f"cdf_{data_model_id.space}_{data_model_id.external_id.lower()}"
view_sqls = generator.generate_views(data_model_id, secret_scope=secret_scope)
```

## Architecture

`cognite.pygen_spark` extends `cognite.pygen`'s architecture:

- **Reuses pygen's View parsing**: Leverages pygen's internal representation of CDF Data Models
- **Custom template engine**: Uses Jinja2 templates to generate UDTF Python code and SQL Views
- **Extends MultiAPIGenerator**: Builds on pygen's code generation infrastructure

See the [Technical Plan](../Technical%20Plan%20-%20CDF%20Databricks%20Integration%20(UDTF-Based).md) for detailed architecture documentation.

## Requirements

- Python 3.9+
- `cognite-pygen` (PyPI package name; import: `cognite.pygen`)
- `cognite-sdk-python` (dependency)
- **Databricks Runtime 18.1+** (for custom dependencies in UDTFs)
  - **Pre-DBR 18.1**: Code generation works on all versions; dependency bundling requires DBR 18.1+

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

## DBR Version Compatibility

This package generates UDTF code that works on all Databricks Runtime versions. However, the ability to bundle Python packages with UDTFs (via `routine_dependencies`) requires DBR 18.1+.

- **Code Generation**: Works on all DBR versions ✅
- **UDTF Templates**: Compatible with all DBR versions ✅
- **Dependency Bundling**: Requires DBR 18.1+ ⚠️

For pre-DBR 18.1 environments, packages must be pre-installed on the cluster. See the [cognite-databricks README](../cognite-databricks/README.md#pre-dbr-181-usage) for details.

## Related Packages

- **[pygen](https://github.com/cognitedata/pygen)**: Base code generation library for CDF Data Models
- **[cognite-databricks](https://github.com/cognitedata/cognite-databricks)**: Helper SDK for UDTF registration and Unity Catalog integration
- **[cognite-sdk-python](https://github.com/cognitedata/cognite-sdk-python)**: Python SDK for CDF APIs

## Documentation

For detailed documentation, see:
- [Technical Plan - CDF Databricks Integration (UDTF-Based)](../Technical%20Plan%20-%20CDF%20Databricks%20Integration%20(UDTF-Based).md)
- [Pygen Developer Documentation](https://cognite-pygen.readthedocs-hosted.com/en/latest/developer_docs/index.html)

## License

[License information]

## Contributing

[Contributing guidelines]

