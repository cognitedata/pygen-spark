# Time Series UDTFs

## Overview

Time series UDTFs allow you to query CDF time series data directly from Spark SQL. These UDTFs are **template-generated** using the same Jinja2 template-based generation approach as Data Model UDTFs, ensuring consistent behavior, error handling, and initialization patterns. They are available in the `pygen-spark` package, making them available for any Spark cluster (not limited to Databricks).

Time series UDTFs work with any Spark cluster that supports PySpark UDTFs, including:
- Standalone Spark clusters
- YARN clusters
- Kubernetes clusters
- Local development environments
- Databricks (via `cognite-databricks` which imports from `pygen-spark`)

## Available Time Series UDTFs

The following time series UDTFs are available:

1. **`time_series_datapoints_udtf`**: Query datapoints from a single time series
2. **`time_series_datapoints_long_udtf`**: Query datapoints from multiple time series in long format
3. **`time_series_latest_datapoints_udtf`**: Get the latest datapoints for one or more time series

## Installation

To use time series UDTFs in a Spark cluster:

```bash
pip install cognite-pygen-spark
```

**Important**: Ensure `cognite-sdk` is also installed on all Spark worker nodes.

## Registration

### Option 1: Generate and Use Template-Generated UDTFs (Recommended)

Time series UDTFs are generated using the same template-based generation as Data Model UDTFs. Generate them using `SparkUDTFGenerator`:

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

# Generate time series UDTFs (template-generated, same as data model UDTFs)
result = generator.generate_time_series_udtfs()
print(f"Generated {result.total_count} time series UDTF(s)")
for udtf_name, file_path in result.generated_files.items():
    print(f"  - {udtf_name}: {file_path}")

# Register the generated UDTFs
from pyspark.sql.functions import udtf
from cognite.databricks import register_udtf_from_file

for udtf_name, file_path in result.generated_files.items():
    register_udtf_from_file(file_path, function_name=udtf_name)
```

**Note**: Time series UDTFs use the same Jinja2 template-based generation as Data Model UDTFs, ensuring consistent behavior, error handling (`_classify_error()`), and initialization patterns (`_create_client()`, `_init_success`).

### Option 2: Use Generated Classes Directly

If you've already generated the UDTF files, you can import and register them directly:

```python
from pyspark.sql.functions import udtf
from cognite.pygen_spark.time_series_udtfs import (
    TimeSeriesDatapointsUDTF,
    TimeSeriesDatapointsLongUDTF,
    TimeSeriesLatestDatapointsUDTF,
)

# Register time series UDTFs
time_series_datapoints_udtf = udtf(TimeSeriesDatapointsUDTF)
time_series_datapoints_long_udtf = udtf(TimeSeriesDatapointsLongUDTF)
time_series_latest_datapoints_udtf = udtf(TimeSeriesLatestDatapointsUDTF)

spark.udtf.register("time_series_datapoints_udtf", time_series_datapoints_udtf)
spark.udtf.register("time_series_datapoints_long_udtf", time_series_datapoints_long_udtf)
spark.udtf.register("time_series_latest_datapoints_udtf", time_series_latest_datapoints_udtf)

print("✓ Time Series UDTFs registered")
```

**Note**: These classes are generated from templates and are included in the `pygen-spark` package for convenience. For customization, use Option 1 to generate your own versions.

## Querying Single Time Series

Query datapoints from a single time series:

```sql
SELECT * FROM time_series_datapoints_udtf(
    instance_id => 'sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround',
    start => '1d-ago',
    end => 'now',
    client_id => 'your-client-id',
    client_secret => 'your-client-secret',
    tenant_id => 'your-tenant-id',
    cdf_cluster => 'westeurope-1',
    project => 'your-project'
) ORDER BY timestamp LIMIT 10;
```

**Parameters:**
- `instance_id`: Instance ID in format "space:external_id" (required)
- `start`: Start time (supports relative times like '1d-ago' or ISO 8601 timestamps)
- `end`: End time (supports 'now' or ISO 8601 timestamps)
- `aggregates`: Optional aggregate type (e.g., "average", "max", "min", "count")
- `granularity`: Optional granularity for aggregates (e.g., "1h", "1d", "30s")
- `client_id`, `client_secret`, `tenant_id`, `cdf_cluster`, `project`: CDF credentials

## Querying Multiple Time Series (Long Format)

Query multiple time series in long format (one row per datapoint). Time series can be from different spaces:

```sql
SELECT * FROM time_series_datapoints_long_udtf(
    instance_ids => 'sailboat:ts1,otherspace:ts2,sailboat:ts3',  -- Format: "space:external_id"
    start => '1d-ago',
    end => 'now',
    client_id => 'your-client-id',
    client_secret => 'your-client-secret',
    tenant_id => 'your-tenant-id',
    cdf_cluster => 'westeurope-1',
    project => 'your-project'
) ORDER BY time_series_external_id, timestamp LIMIT 20;
```

**Parameters:**
- `instance_ids`: Comma-separated string of instance IDs in format `"space:external_id"` (e.g., `"sailboat:ts1,otherspace:ts2"`)
- `start`: Start time (supports relative times like '1d-ago' or ISO 8601 timestamps)
- `end`: End time (supports 'now' or ISO 8601 timestamps)
- `aggregates`: Optional aggregate type (e.g., "average", "max", "min")
- `granularity`: Optional granularity for aggregates (e.g., "1h", "30d")
- `include_aggregate_name`: Whether to include aggregate name in time_series_external_id

**Note**: The `time_series_external_id` in the output is in format `"space:external_id"` to support time series from different spaces.

## Querying Latest Datapoints

Get the latest datapoints for one or more time series. Time series can be from different spaces:

```sql
SELECT * FROM time_series_latest_datapoints_udtf(
    instance_ids => 'sailboat:ts1,otherspace:ts2,sailboat:ts3',  -- Format: "space:external_id"
    before => 'now',
    include_status => true,
    client_id => 'your-client-id',
    client_secret => 'your-client-secret',
    tenant_id => 'your-tenant-id',
    cdf_cluster => 'westeurope-1',
    project => 'your-project'
) ORDER BY time_series_external_id;
```

**Parameters:**
- `instance_ids`: Comma-separated string of instance IDs in format `"space:external_id"` (e.g., `"sailboat:ts1,otherspace:ts2"`)
- `before`: Get latest datapoint before this time (supports 'now' or ISO 8601 timestamps)
- `include_status`: Whether to include status codes in the result

**Note**: The `time_series_external_id` in the output is in format `"space:external_id"` to support time series from different spaces.

## Using with Data Model Views

Time series UDTFs can be joined with Data Model views using `CROSS JOIN LATERAL`:

```sql
SELECT 
    v.external_id AS vessel_id,
    v.name AS vessel_name,
    ts.timestamp,
    ts.value AS speed
FROM vessel_udtf(
    client_id => 'your-client-id',
    client_secret => 'your-client-secret',
    tenant_id => 'your-tenant-id',
    cdf_cluster => 'westeurope-1',
    project => 'your-project',
    name => NULL,
    description => NULL
) v
CROSS JOIN LATERAL (
    SELECT * FROM time_series_datapoints_udtf(
        space => v.space,
        external_id => v.speed_ts_external_id,
        start => '1d-ago',
        end => 'now',
        client_id => 'your-client-id',
        client_secret => 'your-client-secret',
        tenant_id => 'your-tenant-id',
        cdf_cluster => 'westeurope-1',
        project => 'your-project'
    )
) ts
WHERE v.space = 'sailboat'
  AND v.speed_ts_external_id IS NOT NULL
ORDER BY v.external_id, ts.timestamp
LIMIT 100;
```

## Using with Configuration Files

Time series UDTFs work with TOML/YAML configuration files for credentials:

```python
import tomli

# Load credentials from config.toml
with open("config.toml", "rb") as f:
    config = tomli.load(f)

cognite_config = config["cognite"]

# Use in SQL query
query = f"""
SELECT * FROM time_series_datapoints_udtf(
    space => 'sailboat',
    external_id => 'vessel.speed',
    start => '1d-ago',
    end => 'now',
    client_id => '{cognite_config["client_id"]}',
    client_secret => '{cognite_config["client_secret"]}',
    tenant_id => '{cognite_config["tenant_id"]}',
    cdf_cluster => '{cognite_config["cdf_cluster"]}',
    project => '{cognite_config["project"]}'
) ORDER BY timestamp LIMIT 10;
"""
```

## Next Steps

- Learn about [Joining](./joining.md) UDTFs together
- See [Querying](./querying.md) for more query examples
- For Databricks-specific features (Unity Catalog, Secret Manager), see [cognite-databricks documentation](https://github.com/cognitedata/cognite-databricks)

