# Generation

## Overview

`pygen-spark` generates Python UDTF code from CDF Data Models. The generated UDTFs are standard PySpark UDTF classes that can be registered in any Spark cluster.

## Generate UDTFs from a Data Model

### Step 1: Load CDF Client

Load your CDF client from a TOML configuration file:

```python
from cognite.pygen import load_cognite_client_from_toml
from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.pygen_spark import SparkUDTFGenerator
from pathlib import Path

# Load client from TOML file
client = load_cognite_client_from_toml("config.toml")
```

### Step 2: Create Generator

Create a `SparkUDTFGenerator` instance:

```python
# Create generator
generator = SparkUDTFGenerator(
    client=client,
    output_dir=Path("./generated_udtfs"),
    data_model=DataModelId(space="sailboat", external_id="sailboat", version="1"),
    top_level_package="cognite_udtfs",
)
```

**Parameters:**
- `client`: CogniteClient instance
- `output_dir`: Directory where generated UDTF files will be written
- `data_model`: DataModel identifier (DataModelId or DataModel object)
- `top_level_package`: Top-level Python package name for generated code (default: "cognite_databricks")

### Step 3: Generate UDTFs

Generate UDTF code for all Views in the Data Model:

```python
# Generate UDTFs
result = generator.generate_udtfs()

# Access generated files
print(f"Generated {result.total_count} UDTF(s)")
for view_id, file_path in result.generated_files.items():
    print(f"  - {view_id}: {file_path}")
```

## Output Structure

The generator creates a directory structure like this:

```
generated_udtfs/
└── cognite_udtfs/
    ├── SmallBoat_udtf.py
    ├── LargeBoat_udtf.py
    └── ...
```

Each generated file contains:
- A UDTF class with `eval()` method (executes the query)
- An `analyze()` method (defines the output schema)
- An `outputSchema()` method (returns the schema as StructType)
- Helper methods for CDF client creation and filtering

## Generated UDTF Class Structure

Each generated UDTF class follows this pattern:

```python
class SmallBoatUDTF:
    """User-Defined Table Function for SmallBoat View."""
    
    def __init__(self):
        """Initialize UDTF."""
        self.client = None
        self._client_initialized = False
    
    @staticmethod
    def analyze(client_id, client_secret, tenant_id, cdf_cluster, project, ...):
        """Analyze method for PySpark Connect."""
        from pyspark.sql.udtf import AnalyzeResult
        return AnalyzeResult(SmallBoatUDTF.outputSchema())
    
    def eval(self, client_id, client_secret, tenant_id, cdf_cluster, project, ...):
        """Execute UDTF and return rows."""
        # Initialize client, query CDF, yield rows
        ...
    
    @staticmethod
    def outputSchema():
        """Return the output schema."""
        return StructType([...])
```

## Next Steps

After generation, proceed to [Registration](./registration.md) to register the UDTFs in your Spark session.


