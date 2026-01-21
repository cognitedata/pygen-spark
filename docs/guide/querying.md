# Querying UDTFs

## Basic SQL Queries

Once registered, UDTFs can be queried using standard SQL. All UDTFs require CDF credentials as parameters.

⚠️ **Important for Unity Catalog**: When querying Unity Catalog registered UDTFs, **ALL parameters must be explicitly provided in SQL**, even if they have default values in Python. Unity Catalog does not recognize Python default values.

```sql
-- Query a Data Model UDTF (complete example with all parameters)
SELECT * FROM catalog.schema.small_boat_udtf(
    client_id     => SECRET('scope', 'client_id'),
    client_secret => SECRET('scope', 'client_secret'),
    tenant_id     => SECRET('scope', 'tenant_id'),
    cdf_cluster   => SECRET('scope', 'cdf_cluster'),
    project       => SECRET('scope', 'project'),
    name          => NULL,  -- All view property parameters must be provided
    description   => NULL,
    tags          => NULL,
    -- ... all other view property parameters
) LIMIT 10;
```

## Using Configuration Files for Credentials

Instead of hardcoding credentials in SQL, load them from a configuration file:

```python
from cognite.pygen import load_cognite_client_from_toml
import tomli

# Load credentials from TOML file
with open("config.toml", "rb") as f:
    config = tomli.load(f)

cognite_config = config["cognite"]

# Use in SQL query
query = f"""
SELECT * FROM smallboat_udtf(
    '{cognite_config["client_id"]}',
    '{cognite_config["client_secret"]}',
    '{cognite_config["tenant_id"]}',
    '{cognite_config["cdf_cluster"]}',
    '{cognite_config["project"]}',
    NULL,
    NULL
) LIMIT 10;
"""

result = spark.sql(query)
result.show()
```

## Named Parameters

UDTFs support named parameters for cleaner SQL:

```sql
-- Using named parameters (recommended)
SELECT * FROM smallboat_udtf(
    client_id => 'your-client-id',
    client_secret => 'your-client-secret',
    tenant_id => 'your-tenant-id',
    cdf_cluster => 'westeurope-1',
    project => 'your-project',
    name => 'MyBoat',
    description => NULL
) LIMIT 10;
```

## Select All Properties vs Specific Properties

### Select All Properties

```sql
-- Select all properties (SELECT *)
SELECT * FROM smallboat_udtf(
    client_id => 'your-client-id',
    client_secret => 'your-client-secret',
    tenant_id => 'your-tenant-id',
    cdf_cluster => 'westeurope-1',
    project => 'your-project',
    name => NULL,
    description => NULL
) LIMIT 10;
```

### Select Specific Properties

```sql
-- Select specific properties (subset of columns)
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
) LIMIT 10;
```

## Credential Management Best Practices

### Option 1: Environment Variables

Load credentials from environment variables:

```python
import os

client_id = os.getenv("CDF_CLIENT_ID")
client_secret = os.getenv("CDF_CLIENT_SECRET")
tenant_id = os.getenv("CDF_TENANT_ID")
cdf_cluster = os.getenv("CDF_CLUSTER")
project = os.getenv("CDF_PROJECT")

query = f"""
SELECT * FROM smallboat_udtf(
    '{client_id}',
    '{client_secret}',
    '{tenant_id}',
    '{cdf_cluster}',
    '{project}',
    NULL,
    NULL
) LIMIT 10;
"""
```

### Option 2: Configuration Files

Use TOML or YAML configuration files (recommended for development):

```python
from cognite.pygen import load_cognite_client_from_toml
import tomli

with open("config.toml", "rb") as f:
    config = tomli.load(f)

cognite_config = config["cognite"]
# Use cognite_config in queries
```

### Option 3: Spark Configuration

Store credentials in Spark configuration (for cluster-wide access):

```python
# Set in Spark configuration
spark.conf.set("cdf.client_id", "your-client-id")
spark.conf.set("cdf.client_secret", "your-client-secret")
# ... etc

# Retrieve in queries
client_id = spark.conf.get("cdf.client_id")
# Use in SQL
```

**Security Note**: Never commit credentials to version control. Use secure configuration management in production.

## Query Examples

### Basic Query with Filters

```sql
SELECT * FROM smallboat_udtf(
    client_id => 'your-client-id',
    client_secret => 'your-client-secret',
    tenant_id => 'your-tenant-id',
    cdf_cluster => 'westeurope-1',
    project => 'your-project',
    name => 'MyBoat',
    description => NULL
) LIMIT 10;
```

### Query with ORDER BY

```sql
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
ORDER BY name
LIMIT 100;
```

## Next Steps

- Learn about [Filtering](./filtering.md) data with WHERE clauses
- Explore [Joining](./joining.md) UDTFs together


