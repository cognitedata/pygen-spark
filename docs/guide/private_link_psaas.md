# Private Link and PSaaS Setup

This guide explains how to use **cognite-pygen-spark** with CDF **Private Link** or **PSaaS** endpoints.

For the combined Databricks workflow (Unity Catalog, Secret Manager), see the [cognite-databricks Private Link guide](https://github.com/cognitedata/cognite-databricks/blob/main/docs/private_link_psaas.md).

## Background

Private Link customers receive a dedicated CDF base URL from Cognite:

`https://pNNN.plink.<cluster>.cognitedata.com`

Standard clients default to `https://{cdf_cluster}.cognitedata.com`. **cognite-pygen 1.3.0+** adds optional `base_url` in TOML so you can override the API endpoint while keeping `cdf_cluster` for OAuth scopes.

## Requirements

- **cognite-pygen** ≥ 1.3.0
- **cognite-pygen-spark** ≥ 0.3.1
- Network access from your Spark cluster to the Private Link endpoint

```bash
pip install --upgrade "cognite-pygen-spark>=0.3.1" "cognite-pygen>=1.3.0"
```

## TOML configuration

```toml
[cognite]
project = "your-cdf-project"
tenant_id = "your-azure-ad-tenant-id"
cdf_cluster = "westeurope-1"
client_id = "your-oauth2-client-id"
client_secret = "your-oauth2-client-secret"
base_url = "https://p123.plink.westeurope-1.cognitedata.com"
```

| Field | Purpose |
| --- | --- |
| `cdf_cluster` | Public cluster name — used for OAuth scopes (`https://{cluster}.cognitedata.com/.default`) |
| `base_url` | Private Link URL — where API requests are sent |

## Generate UDTFs (provisioning)

Use `load_cognite_client_from_toml()` so `base_url` is applied automatically:

```python
from pathlib import Path

from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.pygen import load_cognite_client_from_toml
from cognite.pygen_spark import SparkUDTFGenerator

client = load_cognite_client_from_toml("config.toml")

generator = SparkUDTFGenerator(
    client=client,
    output_dir=Path("./generated_udtfs"),
    data_model=DataModelId(space="my_space", external_id="MyModel", version="1"),
    top_level_package="cognite_udtfs",
)
result = generator.generate_udtfs()
```

Verify connectivity before generating:

```python
client.iam.token.inspect()
```

## Session registration and querying

When registering UDTFs in a Spark session, pass credentials as usual. Generated UDTFs resolve API URLs from the `cdf_cluster` parameter using the public URL pattern (`https://{cdf_cluster}.cognitedata.com`).

| Phase | Private Link via `base_url` |
| --- | --- |
| Code generation (`load_cognite_client_from_toml`) | Supported |
| UDTF query time (`cdf_cluster` SQL parameter) | Public URL pattern only |

For Private Link-only networks at query time, ensure workers can reach the endpoint UDTFs use, or coordinate with Cognite on runtime `base_url` support.

Example query (public URL resolution at runtime):

```sql
SELECT *
FROM TABLE(
  my_view_udtf(
    client_id => '...',
    client_secret => '...',
    tenant_id => '...',
    cdf_cluster => 'westeurope-1',
    project => 'my-project'
  )
);
```

## `CDFConnectionConfig` vs `load_cognite_client_from_toml`

`CDFConnectionConfig.from_toml()` does not read `base_url` today. For Private Link, use:

```python
from cognite.pygen import load_cognite_client_from_toml

client = load_cognite_client_from_toml("config.toml")
```

## pygen CLI

```bash
pygen generate \
  --space my_space \
  --external-id MyModel \
  --version 1 \
  --tenant-id <tenant-id> \
  --client-id <client-id> \
  --client-secret <client-secret> \
  --cdf-cluster westeurope-1 \
  --cdf-url https://p123.plink.westeurope-1.cognitedata.com \
  --cdf-project my-project
```

## Troubleshooting

| Symptom | Action |
| --- | --- |
| `403` forbidden on public URL | Add `base_url` to TOML for provisioning; check network routing for queries |
| `TypeError: base_url` | Upgrade `cognite-pygen` to ≥ 1.3.0 |
| OAuth OK, API fails | Confirm `base_url` includes `https://` and matches Cognite-provided hostname |

## Related links

- [Installation](./installation.md)
- [Generation](./generation.md)
- [cognite-databricks Private Link guide](https://github.com/cognitedata/cognite-databricks/blob/main/docs/private_link_psaas.md)
- [Configure Private Link (Azure)](https://docs.cognite.com/cdf/access/guides/configure_private_link_azure)
- [cognite-pygen 1.3.0 release](https://github.com/cognitedata/pygen/releases/tag/1.3.0)
