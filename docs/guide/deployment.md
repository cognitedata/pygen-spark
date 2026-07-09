# CDF base URL and TOML deployment

Standalone **cognite-pygen-spark** setup: find your API hostname, write TOML, generate UDTFs.

For Databricks (Unity Catalog, Secret Manager, Views), use the [cognite-databricks deployment guide](https://github.com/cognitedata/cognite-databricks/blob/main/docs/catalog_based/deployment.md).

## Overview

1. **[Find your base URL](#1-i-need-my-base-url)** — cluster row in [Clusters and regions](https://docs.cognite.com/cdf/admin/clusters_regions#cognite-multi-tenant-clusters)
2. **[Write TOML](#2-i-need-toml)** — `[cognite]` credentials; **PSaaS / Private Link:** add `base_url` (`p001.plink.…`)
3. **[Deploy](#3-toml-based-deployment)** — `load_cognite_client_from_toml()` and generate UDTFs
4. **[PSaaS / Private Link](#4-what-psaas-base-url-means)** — when Cognite gave you a Private Link hostname
5. **[Verify on Databricks](#5-verify-on-databricks)** — query Views (Databricks path only)

---

## 1. I need my base URL

Know the **Cognite API URL** for your cluster before deploying.

| Deployment | Where to find base URL |
| --- | --- |
| **Multi-tenant** | [Multi-tenant clusters table](https://docs.cognite.com/cdf/admin/clusters_regions#cognite-multi-tenant-clusters) |
| **Dedicated** | Cognite-provided hostname |
| **PSaaS / Private Link** | Cognite-provided via VPN — [§4](#4-what-psaas-base-url-means) |

Most multi-tenant clusters use `{cluster}.cognitedata.com`. Only **`europe-west1-1`** uses `api.cognitedata.com`.

| Deployment | `base_url` in TOML? |
| --- | --- |
| Multi-tenant (most clusters) | No — `cdf_cluster` suffices |
| Multi-tenant (`europe-west1-1`) | Yes — `https://api.cognitedata.com` |
| Dedicated / PSaaS / Private Link | Yes |

---

## 2. I need TOML

Use TOML for admin setup on standalone Spark — connect to CDF and generate UDTFs via `load_cognite_client_from_toml()`.

**PSaaS / Private Link:** set `base_url` to the Cognite-provided `p001.plink.<cluster>.cognitedata.com` hostname. See [§4](#4-what-psaas-base-url-means).

| Field | Always? | Purpose |
| --- | --- | --- |
| `project`, `tenant_id`, `client_id`, `client_secret`, `cdf_cluster` | Yes | Authentication + OAuth scopes |
| `base_url` | When [§1](#1-i-need-my-base-url) requires it | API hostname override |

Requires **cognite-pygen ≥ 1.3.0**.

### Example — multi-tenant

```toml
# config.toml — do not commit secrets
[cognite]
project = "your-cdf-project"
tenant_id = "your-azure-ad-tenant-id"
cdf_cluster = "westeurope-1"
client_id = "your-oauth2-client-id"
client_secret = "your-oauth2-client-secret"
```

### Example — PSaaS / Private Link

```toml
[cognite]
project = "your-cdf-project"
tenant_id = "your-azure-ad-tenant-id"
cdf_cluster = "az-xyz-001"
client_id = "your-oauth2-client-id"
client_secret = "your-oauth2-client-secret"
base_url = "https://p001.plink.az-xyz-001.cognitedata.com"
```

Full examples: [cognite-databricks `example_config.toml`](https://github.com/cognitedata/cognite-databricks/blob/main/docs/catalog_based/example_config.toml), [`example_config_private_link.toml`](https://github.com/cognitedata/cognite-databricks/blob/main/docs/catalog_based/example_config_private_link.toml).

---

## 3. TOML-based deployment

| Phase | Uses TOML? | What happens |
| --- | --- | --- |
| **1. Prepare config** | Create file | Per [§1](#1-i-need-my-base-url) |
| **2. Install** | No | `pip install cognite-pygen-spark` and `cognite-pygen>=1.3.0` |
| **3. Connect + generate** | **Yes** | `load_cognite_client_from_toml()` → `SparkUDTFGenerator.generate_udtfs()` |
| **4. Register** | No | Register generated UDTF classes |
| **5. Query** | Optional | Pass credentials in SQL |

### Step-by-step

```bash
pip install --upgrade "cognite-pygen-spark>=0.3.1" "cognite-pygen>=1.3.0"
```

```python
from pathlib import Path

from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.pygen import load_cognite_client_from_toml
from cognite.pygen_spark import SparkUDTFGenerator

client = load_cognite_client_from_toml("config.toml")
client.iam.token.inspect()

generator = SparkUDTFGenerator(
    client=client,
    output_dir=Path("./generated_udtfs"),
    data_model=DataModelId(space="my_space", external_id="MyModel", version="1"),
    top_level_package="cognite_udtfs",
)
result = generator.generate_udtfs()
```

See [Registration](./registration.md). For Databricks: [cognite-databricks §3](https://github.com/cognitedata/cognite-databricks/blob/main/docs/catalog_based/deployment.md#3-toml-based-deployment).

---

## 4. What PSaaS base URL means

**Private SaaS (PSaaS)** and **Private Link** use a Cognite-provided hostname over your private network instead of the public cluster URL.

Typical hostname: `p001.plink.az-xyz-001.cognitedata.com`

```toml
[cognite]
project = "your-cdf-project"
tenant_id = "your-azure-ad-tenant-id"
cdf_cluster = "az-xyz-001"
base_url = "https://p001.plink.az-xyz-001.cognitedata.com"
client_id = "your-oauth2-client-id"
client_secret = "your-oauth2-client-secret"
```

| Field | Purpose |
| --- | --- |
| `cdf_cluster` | OAuth scopes |
| `base_url` | API requests over your VPN |

Networking: [Private Link on Azure](https://docs.cognite.com/cdf/access/guides/configure_private_link_azure), [Private Link on AWS](https://docs.cognite.com/cdf/access/guides/configure_private_link_aws).

---

## 5. Verify on Databricks

On **Databricks**, deployment succeeded when you can `SELECT` from **Unity Catalog Views** — not UDTFs, not TOML.

See [Verify deployment (Databricks)](https://github.com/cognitedata/cognite-databricks/blob/main/docs/catalog_based/deployment.md#5-verify-deployment-databricks).

---

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

## `CDFConnectionConfig` vs `load_cognite_client_from_toml`

`CDFConnectionConfig.from_toml()` does not read `base_url`. Use `load_cognite_client_from_toml()`.

## pygen CLI

```bash
pygen generate \
  --cdf-cluster az-xyz-001 \
  --cdf-url https://p001.plink.az-xyz-001.cognitedata.com \
  --cdf-project my-project \
  ...
```

## Related links

- [Clusters and regions](https://docs.cognite.com/cdf/admin/clusters_regions#clusters-and-regions)
- [Installation](./installation.md)
- [cognite-databricks deployment guide](https://github.com/cognitedata/cognite-databricks/blob/main/docs/catalog_based/deployment.md)
