# Private Link and PSaaS Setup

Configuration guide for **cognite-pygen-spark** on standalone Spark clusters when CDF is **not** on the default public base URL.

For **Databricks**, follow the [cognite-databricks guide](https://github.com/cognitedata/cognite-databricks/blob/main/docs/private_link_psaas.md) — analysts query **Views** after admin setup, not UDTFs.

## How this guide fits together

| Step | Your question | Section |
| --- | --- | --- |
| 1 | **What kind of deployment do I use?** | [Which deployment do you use?](#1-which-deployment-do-you-use) |
| 2 | **Why do I need TOML-based deployment?** | [Why TOML-based deployment?](#2-why-toml-based-deployment) |
| 3 | **What does base URL mean for my deployment type?** | [What base URL means — three types](#3-what-base-url-means--three-types) |
| 4 | **How do I deploy with TOML?** | [TOML-based deployment](#4-toml-based-deployment) |
| 5 | **What does PSaaS base URL mean specifically?** | [What PSaaS base URL means](#5-what-psaas-base-url-means) |
| 6 | **(Databricks) How do I know deployment succeeded?** | [Verify on Databricks](#6-verify-on-databricks) |

---

## 1. Which deployment do you use?

| Deployment | How you recognize it | This guide? |
| --- | --- | --- |
| **Multi-tenant** | Cluster on the [public cluster list](https://docs.cognite.com/cdf/admin/clusters_regions#cognite-multi-tenant-clusters) | **No** — see [Installation](./installation.md) |
| **Dedicated** | Customer-specific hostname from Cognite | **Yes** |
| **PSaaS / Private Link** | Private Link hostname via your VPN (e.g. `p001.plink.az-xyz-001.cognitedata.com`) | **Yes** |

---

## 2. Why TOML-based deployment?

You need **TOML-based deployment** because you are **not using the default base URL** (`https://{cdf_cluster}.cognitedata.com`).

For **dedicated** and **PSaaS / Private Link**, Cognite gives you a different hostname. Put `base_url` in TOML and call `load_cognite_client_from_toml()` during admin setup so the SDK connects to the right endpoint when generating UDTFs.

On standalone Spark, you may also read credential fields from TOML when querying UDTFs. Requires **cognite-pygen ≥ 1.3.0**.

---

## 3. What base URL means — three types

See [Clusters and regions](https://docs.cognite.com/cdf/admin/clusters_regions#clusters-and-regions).

### 1. Multi-tenant cluster

Shared Cognite cluster from the [published list](https://docs.cognite.com/cdf/admin/clusters_regions#cognite-multi-tenant-clusters).

| | |
| --- | --- |
| **Base URL** | Listed in Clusters and regions (typically `https://{cluster}.cognitedata.com`) |
| **TOML** | `cdf_cluster` only — no `base_url` |

### 2. Dedicated cluster

Exclusive resources on a Cognite-managed dedicated cluster.

| | |
| --- | --- |
| **Base URL** | Customer-specific hostname **provided by Cognite** |
| **TOML** | `cdf_cluster` **and** `base_url` |

### 3. PSaaS / Private Link

Cognite-provided hostname wired into your VPN. See [§5](#5-what-psaas-base-url-means).

| | |
| --- | --- |
| **Base URL** | Cognite-provided Private Link hostname |
| **TOML** | `cdf_cluster` (OAuth) **and** `base_url` (API endpoint) |

### Summary

| Deployment | Base URL source | `cdf_cluster` | `base_url` |
| --- | --- | --- | --- |
| **Multi-tenant** | [Published cluster list](https://docs.cognite.com/cdf/admin/clusters_regions#cognite-multi-tenant-clusters) | Required | Not needed |
| **Dedicated** | Cognite-provided, customer-specific | Required | Required |
| **PSaaS / Private Link** | Cognite-provided; routed via customer VPN | Required | Required |

---

## 4. TOML-based deployment

On standalone Spark, TOML is used during **admin setup** to connect to CDF and generate UDTF code. Build TOML per [§3](#3-what-base-url-means--three-types).

| Phase | Uses TOML? | What happens |
| --- | --- | --- |
| **1. Prepare config** | Create file | TOML per deployment type (`base_url` when not default) |
| **2. Install** | No | `pip install cognite-pygen-spark` and `cognite-pygen>=1.3.0` |
| **3. Connect + generate** | **Yes** | `load_cognite_client_from_toml()` → `SparkUDTFGenerator.generate_udtfs()` |
| **4. Register** | No | Import and register generated UDTF classes |
| **5. Query** | Optional | Pass credentials in SQL; read from TOML or env vars |

### Step-by-step

**Step 1 — Install** (driver and all workers need `cognite-sdk`):

```bash
pip install --upgrade "cognite-pygen-spark>=0.3.1" "cognite-pygen>=1.3.0"
```

**Step 2 — Create `config.toml`** per [§3](#3-what-base-url-means--three-types). See [TOML configuration](#toml-configuration).

**Step 3 — Generate UDTFs:**

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

**Step 4 — Register** ([Registration](./registration.md)).

**Step 5 — Query** with credentials as SQL parameters:

```python
import toml

config = toml.load("config.toml")["cognite"]
```

`base_url` from TOML applies only during **generation** (step 3), not at query time.

For Databricks (TOML → Secret Manager → Views), see the [cognite-databricks §4](https://github.com/cognitedata/cognite-databricks/blob/main/docs/private_link_psaas.md#4-toml-based-deployment).

---

## 5. What PSaaS base URL means

**Private SaaS (PSaaS)** and **Private Link** use a Cognite-provided per-customer hostname routed through **your private network** instead of `https://{cdf_cluster}.cognitedata.com`.

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
| `cdf_cluster` | OAuth scopes (cluster name Cognite assigned) |
| `base_url` | Where API requests go — Private Link hostname over your VPN |

Networking: [Private Link on Azure](https://docs.cognite.com/cdf/access/guides/configure_private_link_azure), [Private Link on AWS](https://docs.cognite.com/cdf/access/guides/configure_private_link_aws).

---

## 6. Verify on Databricks

On **Databricks**, deployment succeeded when you can `SELECT` from **Unity Catalog Views** — you do **not** call UDTFs or use TOML at query time.

See [Verify deployment (Databricks)](https://github.com/cognitedata/cognite-databricks/blob/main/docs/private_link_psaas.md#6-verify-deployment-databricks) in the cognite-databricks guide.

---

## Requirements

- **cognite-pygen** ≥ 1.3.0
- **cognite-pygen-spark** ≥ 0.3.1
- Network access from your Spark cluster to the Private Link endpoint

## TOML configuration

Reference for `[cognite]`. Fields depend on [deployment type](#3-what-base-url-means--three-types).

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

`CDFConnectionConfig.from_toml()` does not read `base_url`. For PSaaS / Private Link, use:

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
| `403` forbidden on public URL | Add `base_url` to TOML; check network routing |
| `TypeError: base_url` | Upgrade `cognite-pygen` to ≥ 1.3.0 |
| OAuth OK, API fails | Confirm `base_url` includes `https://` |

## Related links

- [Clusters and regions](https://docs.cognite.com/cdf/admin/clusters_regions#clusters-and-regions)
- [Installation](./installation.md)
- [Generation](./generation.md)
- [cognite-databricks Private Link guide](https://github.com/cognitedata/cognite-databricks/blob/main/docs/private_link_psaas.md)
- [cognite-pygen 1.3.0 release](https://github.com/cognitedata/pygen/releases/tag/1.3.0)
