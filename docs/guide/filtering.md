# Filtering

## WHERE Clauses in SQL

UDTFs support filtering via WHERE clauses in SQL. The filters are pushed down to the CDF API call, improving performance:

```sql
-- Filter by external_id
SELECT * FROM smallboat_udtf(
    client_id => 'your-client-id',
    client_secret => 'your-client-secret',
    tenant_id => 'your-tenant-id',
    cdf_cluster => 'westeurope-1',
    project => 'your-project',
    name => NULL,
    description => NULL
)
WHERE external_id = 'my-boat-123'
LIMIT 10;
```

## Predicate Pushdown

Predicate pushdown means filter conditions are sent to CDF (`instances/list` /
`instances/aggregate`) from the generated UDTF instead of only being applied in Spark
after over-fetching.

**Pushed when bound as UDTF parameters:**

| Mechanism | CDF |
|-----------|-----|
| Non-null view-property arg | `equals` / `in` / `containsAny` |
| `_exists` / `_not_exists` | `exists` / `not.exists` |
| `instance_space` / `external_id` args | identity `["node\|edge", "space\|externalId"]` |
| `_gt` / `_gte` / `_lt` / `_lte` | `range` |
| `_row_limit` (no `ORDER BY`) | list `limit` + stop pagination |
| `_query_mode='aggregate'` + `_aggregates` | `instances/aggregate` |

A SQL `WHERE` on top of a view that always passes `prop => NULL` is **not** automatically
pushed; bind parameters on the UDTF call or use cognite-databricks `DataModelQueryRewriter`.

**Instance space** (output column `space`) is not the view model space used in property paths.

Related: [GitHub #68](https://github.com/cognitedata/pygen-spark/issues/68),
[#69](https://github.com/cognitedata/pygen-spark/issues/69).

## Filter Examples

### Equality Filters

```sql
-- Filter by single property - select all properties
SELECT * FROM smallboat_udtf(
    client_id => 'your-client-id',
    client_secret => 'your-client-secret',
    tenant_id => 'your-tenant-id',
    cdf_cluster => 'westeurope-1',
    project => 'your-project',
    name => NULL,
    description => NULL
)
WHERE name = 'MyBoat'
LIMIT 10;

-- Filter by single property - select specific properties
SELECT 
    external_id,
    name,
    space
FROM smallboat_udtf(
    client_id => 'your-client-id',
    client_secret => 'your-client-secret',
    tenant_id => 'your-tenant-id',
    cdf_cluster => 'westeurope-1',
    project => 'your-project',
    name => NULL,
    description => NULL
)
WHERE name = 'MyBoat'
LIMIT 10;

-- Filter by space and external_id
SELECT * FROM vessel_udtf(
    client_id => 'your-client-id',
    client_secret => 'your-client-secret',
    tenant_id => 'your-tenant-id',
    cdf_cluster => 'westeurope-1',
    project => 'your-project',
    name => NULL,
    description => NULL
)
WHERE space = 'sailboat' AND external_id = 'vessel-123'
LIMIT 10;
```

### Range Filters

```sql
-- Filter by timestamp range
SELECT * FROM pump_view_udtf(
    client_id => 'your-client-id',
    client_secret => 'your-client-secret',
    tenant_id => 'your-tenant-id',
    cdf_cluster => 'westeurope-1',
    project => 'your-project',
    name => NULL,
    description => NULL
)
WHERE timestamp > '2025-01-01' AND timestamp < '2025-12-31'
ORDER BY timestamp;

-- Filter by numeric range
SELECT * FROM sensor_udtf(
    client_id => 'your-client-id',
    client_secret => 'your-client-secret',
    tenant_id => 'your-tenant-id',
    cdf_cluster => 'westeurope-1',
    project => 'your-project',
    name => NULL,
    description => NULL
)
WHERE value > 100 AND value < 200
LIMIT 10;
```

### NULL Handling

```sql
-- Filter out NULL values
SELECT * FROM smallboat_udtf(
    client_id => 'your-client-id',
    client_secret => 'your-client-secret',
    tenant_id => 'your-tenant-id',
    cdf_cluster => 'westeurope-1',
    project => 'your-project',
    name => NULL,
    description => NULL
)
WHERE description IS NOT NULL
LIMIT 10;

-- Find records with NULL values
SELECT * FROM vessel_udtf(
    client_id => 'your-client-id',
    client_secret => 'your-client-secret',
    tenant_id => 'your-tenant-id',
    cdf_cluster => 'westeurope-1',
    project => 'your-project',
    name => NULL,
    description => NULL
)
WHERE name IS NULL
LIMIT 10;
```

### Multiple Conditions

```sql
-- Complex filtering with multiple conditions
SELECT * FROM pump_view_udtf(
    client_id => 'your-client-id',
    client_secret => 'your-client-secret',
    tenant_id => 'your-tenant-id',
    cdf_cluster => 'westeurope-1',
    project => 'your-project',
    name => NULL,
    description => NULL
)
WHERE space = 'power'
  AND timestamp > '2025-01-01'
  AND status = 'active'
  AND value > 50
ORDER BY timestamp DESC
LIMIT 100;
```

## Next Steps

- Learn about [Joining](./joining.md) UDTFs together
- See [Querying](./querying.md) for more query examples


