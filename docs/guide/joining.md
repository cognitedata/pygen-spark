# Joining UDTFs

## Joining on external_id

The most common join pattern is joining UDTFs based on `external_id`:

```sql
-- Join two UDTFs on external_id
SELECT 
    v.external_id,
    v.name AS vessel_name,
    s.external_id AS sensor_id,
    s.value AS sensor_value
FROM vessel_udtf(
    client_id => 'your-client-id',
    client_secret => 'your-client-secret',
    tenant_id => 'your-tenant-id',
    cdf_cluster => 'westeurope-1',
    project => 'your-project',
    name => NULL,
    description => NULL
) v
JOIN sensor_udtf(
    client_id => 'your-client-id',
    client_secret => 'your-client-secret',
    tenant_id => 'your-tenant-id',
    cdf_cluster => 'westeurope-1',
    project => 'your-project',
    name => NULL,
    description => NULL
) s ON v.external_id = s.vessel_external_id
WHERE v.space = 'sailboat'
LIMIT 10;
```

## Joining on space + external_id

For more precise joins, combine `space` and `external_id`:

```sql
-- Join on space and external_id (more precise)
SELECT 
    a.external_id,
    a.space,
    b.parent_external_id,
    b.child_external_id
FROM parent_udtf(
    client_id => 'your-client-id',
    client_secret => 'your-client-secret',
    tenant_id => 'your-tenant-id',
    cdf_cluster => 'westeurope-1',
    project => 'your-project',
    name => NULL,
    description => NULL
) a
JOIN child_udtf(
    client_id => 'your-client-id',
    client_secret => 'your-client-secret',
    tenant_id => 'your-tenant-id',
    cdf_cluster => 'westeurope-1',
    project => 'your-project',
    name => NULL,
    description => NULL
) b 
    ON a.space = b.space 
    AND a.external_id = b.parent_external_id
LIMIT 10;
```

## CROSS JOIN LATERAL Patterns

`CROSS JOIN LATERAL` is useful for dynamic queries where one UDTF's output determines the parameters for another UDTF. This is particularly useful when joining with time series UDTFs.

**Note**: Time series UDTFs are available in the `cognite-databricks` package. If you're using basic Spark clusters, you may need to implement similar UDTFs or use the ones from `cognite-databricks` if they're compatible with your Spark version.

```sql
-- Use vessel data to query related time series
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

## Joining with Data Model Views

You can join UDTFs with Data Model views that contain time series references:

```sql
-- Join vessel view with time series using instance_id from view
SELECT 
    v.external_id AS vessel_id,
    v.name AS vessel_name,
    v.speed_ts_space,
    v.speed_ts_external_id,
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
        space => v.speed_ts_space,
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
ORDER BY v.external_id, ts.timestamp;
```

## Querying Multiple Time Series from Data Model Views

When a Data Model view contains references to multiple time series, you can query them all:

```sql
-- Query multiple time series referenced in a view
SELECT 
    v.external_id AS vessel_id,
    v.name AS vessel_name,
    speed_ts.timestamp AS speed_timestamp,
    speed_ts.value AS speed_value,
    course_ts.timestamp AS course_timestamp,
    course_ts.value AS course_value
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
        space => v.speed_ts_space,
        external_id => v.speed_ts_external_id,
        start => '1d-ago',
        end => 'now',
        client_id => 'your-client-id',
        client_secret => 'your-client-secret',
        tenant_id => 'your-tenant-id',
        cdf_cluster => 'westeurope-1',
        project => 'your-project'
    )
) speed_ts
CROSS JOIN LATERAL (
    SELECT * FROM time_series_datapoints_udtf(
        space => v.course_ts_space,
        external_id => v.course_ts_external_id,
        start => '1d-ago',
        end => 'now',
        client_id => 'your-client-id',
        client_secret => 'your-client-secret',
        tenant_id => 'your-tenant-id',
        cdf_cluster => 'westeurope-1',
        project => 'your-project'
    )
) course_ts
WHERE v.space = 'sailboat'
  AND v.speed_ts_external_id IS NOT NULL
  AND v.course_ts_external_id IS NOT NULL
ORDER BY v.external_id, speed_ts.timestamp;
```

## Next Steps

- Learn about [Filtering](./filtering.md) data with WHERE clauses
- See [Time Series](./time_series.md) for more details on time series UDTFs


