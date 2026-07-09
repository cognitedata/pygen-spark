# Installation

## Prerequisites

- **Spark Cluster**: Access to a Spark cluster (standalone, YARN, Kubernetes, or local)
- **Python 3.9+**: Python 3.9 or higher
- **PySpark 3.5+**: PySpark 3.5 or higher (required for UDTF support)
- **CDF Credentials**: Access to CDF credentials (client_id, client_secret, tenant_id, cdf_cluster, project)
- **CDF Data Model**: A CDF Data Model with Views (for Data Model UDTFs)

## Install pygen-spark

Install `cognite-pygen-spark` and its dependencies:

```bash
pip install cognite-pygen-spark
```

This will automatically install:
- `cognite-pygen` (base code generation library)
- `cognite-sdk` (CDF Python SDK)
- `jinja2` (template engine)

## Install Dependencies on Spark Cluster

**Important**: The `cognite-sdk` package must be installed on all Spark worker nodes. The generated UDTF code requires `cognite-sdk` to connect to CDF.

### Option 1: Pre-install on All Nodes

Install `cognite-sdk` on all Spark worker nodes:

```bash
# On each worker node
pip install cognite-sdk
```

### Option 2: Use Spark's Python Environment

If your Spark cluster uses a shared Python environment, install there:

```bash
# In the Spark Python environment
pip install cognite-sdk
```

### Option 3: Package Dependencies (Advanced)

For production deployments, you can package dependencies with your application. See your Spark cluster's documentation for details.

## Verify Installation

```python
from cognite.pygen_spark import SparkUDTFGenerator
from cognite.pygen import load_cognite_client_from_toml

# Verify imports work
print("✓ pygen-spark installed successfully")
```

## Configuration File Setup

Create a TOML configuration file (`config.toml`) with your CDF credentials:

```toml
[cognite]
project = "<your-cdf-project>"
tenant_id = "<your-tenant-id>"
cdf_cluster = "<your-cdf-cluster>"  # Multi-tenant: see https://docs.cognite.com/cdf/admin/clusters_regions#cognite-multi-tenant-clusters
client_id = "<your-client-id>"
client_secret = "<your-client-secret>"
```

**Dedicated / Private Link / PSaaS:** also set `base_url` with the Cognite-provided hostname (wired via your VPN for PSaaS/Private Link). Requires **cognite-pygen** ≥ 1.3.0. See [CDF base URL types](./private_link_psaas.md#cdf-base-url).

**Security Note**: Keep your `config.toml` file secure and never commit it to version control. Use environment variables or secure configuration management in production.

## Verify PySpark Version

Check that your PySpark version supports UDTFs (3.5+):

```python
import pyspark
print(f"PySpark version: {pyspark.__version__}")

# Should be 3.5.0 or higher
assert pyspark.__version__ >= "3.5.0", "PySpark 3.5+ required for UDTF support"
```

## Next Steps

Once installation is complete, proceed to [Generation](./generation.md) to generate UDTF code from your CDF Data Model.


