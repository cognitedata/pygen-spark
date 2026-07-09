# Private Link and PSaaS Setup

This guide explains CDF **base URL** configuration for **cognite-pygen-spark**, with setup details for deployments that require a custom `base_url` in TOML.

For the combined Databricks workflow (Unity Catalog, Secret Manager), see the [cognite-databricks Private Link guide](https://github.com/cognitedata/cognite-databricks/blob/main/docs/private_link_psaas.md).

## CDF base URL

How you connect to CDF depends on your deployment type. See [Clusters and regions](https://docs.cognite.com/cdf/admin/clusters_regions#clusters-and-regions).

### 1. Multi-tenant cluster

Your organization runs on a **shared** Cognite cluster. Choose from the [published multi-tenant list](https://docs.cognite.com/cdf/admin/clusters_regions#cognite-multi-tenant-clusters).

| | |
| --- | --- |
| **Base URL** | Listed in the Clusters and regions table |
| **Who provides it** | Cognite — fixed per cluster from the public list |
| **TOML** | `cdf_cluster` only — no `base_url` |

### 2. Dedicated cluster

Your organization uses **exclusive** resources on a Cognite-managed dedicated cluster (contact your Cognite representative).

| | |
| --- | --- |
| **Base URL** | Customer-specific hostname **provided by Cognite** |
| **Who provides it** | Cognite — assigned to your organization |
| **TOML** | `cdf_cluster` **and** `base_url` |

### 3. PSaaS / Private Link

**Private SaaS (PSaaS)** and **Private Link** use a **Cognite-provided** per-customer hostname that is **wired into your VPN or private network setup**. API traffic reaches CDF through your private connectivity instead of the shared public cluster URL.

Typical hostname format:

`p001.plink.az-xyz-001.cognitedata.com`

| | |
| --- | --- |
| **Base URL** | Cognite-provided Private Link hostname (for example `https://p001.plink.az-xyz-001.cognitedata.com`) |
| **Who provides it** | Cognite assigns the URL; you integrate it with your VPN / Private Link configuration |
| **TOML** | `cdf_cluster` (for OAuth) **and** `base_url` (Cognite-provided Private Link URL) |

### Summary

| Deployment | Base URL source | `cdf_cluster` | `base_url` |
| --- | --- | --- | --- |
| **Multi-tenant** | [Published cluster list](https://docs.cognite.com/cdf/admin/clusters_regions#cognite-multi-tenant-clusters) | Required | Not needed |
| **Dedicated** | Cognite-provided, customer-specific | Required | Required |
| **PSaaS / Private Link** | Cognite-provided; routed via customer VPN | Required | Required |

**cognite-pygen 1.3.0+** supports `base_url` in TOML via `load_cognite_client_from_toml()` for dedicated, PSaaS, and Private Link deployments.

## When you need this guide

Multi-tenant customers only need `cdf_cluster` — see [Installation](./installation.md) and [Generation](./generation.md).

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
cdf_cluster = "az-xyz-001"
client_id = "your-oauth2-client-id"
client_secret = "your-oauth2-client-secret"
base_url = "https://p001.plink.az-xyz-001.cognitedata.com"
```

| Field | Purpose |
| --- | --- |
| `cdf_cluster` | Cluster name — used for OAuth scopes (`https://{cluster}.cognitedata.com/.default`) |
| `base_url` | Cognite-provided Private Link URL — routed via your VPN; where API requests are sent |

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
    cdf_cluster => 'az-xyz-001',
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
  --cdf-cluster az-xyz-001 \
  --cdf-url https://p001.plink.az-xyz-001.cognitedata.com \
  --cdf-project my-project
```

## Troubleshooting

| Symptom | Action |
| --- | --- |
| `403` forbidden on public URL | Add `base_url` to TOML for provisioning; check network routing for queries |
| `TypeError: base_url` | Upgrade `cognite-pygen` to ≥ 1.3.0 |
| OAuth OK, API fails | Confirm `base_url` includes `https://` and matches Cognite-provided hostname |

## Related links

- [Clusters and regions](https://docs.cognite.com/cdf/admin/clusters_regions#clusters-and-regions) — standard CDF base URLs by cluster
- [Installation](./installation.md)
- [Generation](./generation.md)
- [cognite-databricks Private Link guide](https://github.com/cognitedata/cognite-databricks/blob/main/docs/private_link_psaas.md)
- [Configure Private Link (Azure)](https://docs.cognite.com/cdf/access/guides/configure_private_link_azure)
- [cognite-pygen 1.3.0 release](https://github.com/cognitedata/pygen/releases/tag/1.3.0)
