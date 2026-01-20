# Registration

## Overview

UDTF registration is performed using **Unity Catalog SQL** to ensure compatibility with SQL Warehouses and serverless execution. Generated UDTF code is registered via `CREATE FUNCTION ... AS $$...$$` and runs in scalar mode.

## Unity Catalog Registration (SQL)

Use `cognite-databricks` to register generated UDTFs via SQL:

```python
from cognite.databricks import generate_udtf_notebook
from cognite.pygen import load_cognite_client_from_toml
from cognite.client.data_classes.data_modeling.ids import DataModelId

client = load_cognite_client_from_toml("config.toml")

data_model_id = DataModelId(space="sailboat", external_id="sailboat", version="1")

generator = generate_udtf_notebook(
    data_model_id,
    client,
    catalog="main",
    schema="cdf_models",
    output_dir="/Workspace/Users/user@example.com/udtf",
)

udtf_result = generator.register_udtfs(
    secret_scope="cdf_sailboat_sailboat",
    if_exists="replace",
)

print(f"âœ“ Registered {udtf_result.total_count} UDTF(s)")
```

## SQL Usage

After registration, use the UDTF in SQL:

```sql
SELECT * FROM main.cdf_models.small_boat_udtf(
    client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project => SECRET('cdf_sailboat_sailboat', 'project')
)
```

## Notes

- UDTFs run in **scalar mode** for serverless SQL compatibility.
- Functions are persistent in Unity Catalog and visible to SQL Warehouses and BI tools.
