# Time Series UDTFs

## Overview

Time series UDTFs allow you to query CDF time series data directly from Spark SQL. These UDTFs are pre-built and available in the `cognite-databricks` package.

**Note**: Time series UDTFs are part of the `cognite-databricks` package, which is designed for Databricks environments. However, the UDTF code itself is generic PySpark code and may work in basic Spark clusters if:

1. The `cognite-databricks` package is installed
2. All dependencies are available on Spark worker nodes
3. The Spark version supports the required UDTF features

## Available Time Series UDTFs

The following time series UDTFs are available:

1. **`time_series_datapoints_udtf`**: Query datapoints from a single time series
2. **`time_series_datapoints_long_udtf`**: Query datapoints from multiple time series in long format
3. **`time_series_latest_datapoints_udtf`**: Get the latest datapoints for one or more time series

## Installation

To use time series UDTFs in a basic Spark cluster:

```bash
pip install cognite-databricks
```

**Important**: Ensure `cognite-sdk` is also installed on all Spark worker nodes.

## Registration

Register time series UDTFs in your Spark session:

```python
from pyspark.sql.functions import udtf
from cognite.databricks.time_series_udtfs import (
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

## Querying Single Time Series

Query datapoints from a single time series:

```sql
SELECT * FROM time_series_datapoints_udtf(
    space => 'sailboat',
    external_id => 'vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround',
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
- `space`: Space name for the time series
- `external_id`: External ID of the time series
- `start`: Start time (supports relative times like '1d-ago' or ISO 8601 timestamps)
- `end`: End time (supports 'now' or ISO 8601 timestamps)
- `client_id`, `client_secret`, `tenant_id`, `cdf_cluster`, `project`: CDF credentials

## Querying Multiple Time Series (Long Format)

Query multiple time series in long format (one row per datapoint):

```sql
SELECT * FROM time_series_datapoints_long_udtf(
    space => 'sailboat',
    external_ids => 'ts1,ts2,ts3',  -- Comma-separated string
    start => '1d-ago',
    end => 'now',
    client_id => 'your-client-id',
    client_secret => 'your-client-secret',
    tenant_id => 'your-tenant-id',
    cdf_cluster => 'westeurope-1',
    project => 'your-project'
) ORDER BY time_series_external_id, timestamp LIMIT 20;
```

**Note**: `external_ids` is a comma-separated string, not an array.

## Querying Latest Datapoints

Get the latest datapoints for one or more time series:

```sql
SELECT * FROM time_series_latest_datapoints_udtf(
    space => 'sailboat',
    external_ids => 'ts1,ts2,ts3',  -- Comma-separated string
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
- `space`: Space name (single string, not array)
- `external_ids`: Comma-separated string of external IDs
- `before`: Get latest datapoint before this time (supports 'now' or ISO 8601 timestamps)
- `include_status`: Whether to include status codes in the result

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

## Compatibility Notes

- **Basic Spark Clusters**: Time series UDTFs may work in basic Spark clusters if all dependencies are installed, but they are primarily designed for Databricks environments.

- **Alternative**: If time series UDTFs from `cognite-databricks` don't work in your Spark cluster, you can create similar UDTFs using the same patterns as the generated Data Model UDTFs.

## Next Steps

- Learn about [Joining](./joining.md) UDTFs together
- See [Querying](./querying.md) for more query examples
- For Databricks-specific features, see [cognite-databricks documentation](https://github.com/cognitedata/cognite-databricks)

