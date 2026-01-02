# Troubleshooting

## Common Issues and Solutions

### Issue: "No active SparkSession found"

**Solution**: Ensure you're running the code in a Spark environment with an active Spark session. Create a SparkSession if needed:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("UDTF Registration").getOrCreate()
```

### Issue: "PySpark is required for UDTF registration"

**Solution**: Install PySpark (version 3.5+ required for UDTF support):

```bash
pip install pyspark>=3.5.0
```

### Issue: "ModuleNotFoundError: No module named 'cognite'"

**Solution**: Install `cognite-sdk` on all Spark worker nodes. The generated UDTF code requires `cognite-sdk` to connect to CDF.

**For Standalone Spark Clusters:**
1. Install `cognite-sdk` on each worker node:
   ```bash
   pip install cognite-sdk
   ```

2. Or use Spark's `--py-files` option to distribute dependencies:
   ```bash
   spark-submit --py-files cognite-sdk.whl your_script.py
   ```

**For Local Development:**
```bash
pip install cognite-sdk
```

### Issue: UDTF returns no results

**Possible Causes:**
1. **Incorrect credentials**: Verify that credentials in your config file are correct
2. **No matching data**: Check that filters match existing data in CDF
3. **View doesn't exist**: Verify the Data Model and View exist in CDF

**Debug Steps:**
```python
# Test credentials
from cognite.pygen import load_cognite_client_from_toml
client = load_cognite_client_from_toml("config.toml")

# Test data model query
from cognite.client.data_classes.data_modeling.ids import DataModelId
data_model_id = DataModelId(space="sailboat", external_id="sailboat", version="1")
data_models = client.data_modeling.data_models.retrieve(data_model_id)
print(f"Data model exists: {data_models is not None}")

# Test instances
instances = client.data_modeling.instances.list(
    sources=data_models[0].views[0].as_id(),
    limit=10
)
print(f"Found {len(instances)} instances")
```

### Issue: "Module not found" errors on worker nodes

**Solution**: Ensure all dependencies are installed on Spark worker nodes. For standalone clusters:

1. **Option 1**: Install on each worker node manually
   ```bash
   # On each worker node
   pip install cognite-sdk
   ```

2. **Option 2**: Use Spark's dependency distribution
   ```python
   # Package dependencies
   spark.sparkContext.addPyFile("path/to/cognite-sdk.whl")
   ```

3. **Option 3**: Use `--py-files` with spark-submit
   ```bash
   spark-submit --py-files cognite-sdk.whl your_script.py
   ```

### Issue: UDTF registration succeeds but SQL query fails

**Possible Causes:**
1. **Function name mismatch**: Verify the registered function name matches what you're calling in SQL
2. **Parameter mismatch**: Check that all required parameters are provided
3. **Type errors**: Ensure parameter types match the UDTF's expected types
4. **Credential errors**: Verify credentials are passed correctly

**Debug Steps:**
```python
# Check registered functions
registered_udtfs = spark.catalog.listFunctions()
udtf_names = [f.name for f in registered_udtfs if "udtf" in f.name.lower()]
print("Registered UDTFs:", udtf_names)

# Verify function name in SQL matches
# If registered as "smallboat_udtf", use: SELECT * FROM smallboat_udtf(...)
```

### Issue: Configuration file not found

**Solution**: Ensure your `config.toml` file is in the correct location and accessible:

```python
from pathlib import Path

config_path = Path("config.toml")
if not config_path.exists():
    raise FileNotFoundError(f"Config file not found: {config_path}")

# Use absolute path if needed
config_path = Path("/absolute/path/to/config.toml")
```

### Issue: Credentials in SQL queries are exposed

**Solution**: Never hardcode credentials in SQL queries. Use configuration files or environment variables:

```python
# Good: Load from config file
from cognite.pygen import load_cognite_client_from_toml
import tomli

with open("config.toml", "rb") as f:
    config = tomli.load(f)

cognite_config = config["cognite"]

# Use in query
query = f"""
SELECT * FROM smallboat_udtf(
    '{cognite_config["client_id"]}',
    '{cognite_config["client_secret"]}',
    ...
)
"""
```

### Issue: UDTF class not found when importing

**Solution**: Ensure the generated code directory is in Python's path:

```python
import sys
from pathlib import Path

# Add generated code directory to path
udtf_dir = Path("./generated_udtfs")
sys.path.insert(0, str(udtf_dir))

# Now import should work
from cognite_udtfs.SmallBoat_udtf import SmallBoatUDTF
```

### Issue: PySpark version too old

**Solution**: UDTFs require PySpark 3.5.0 or higher. Check your version:

```python
import pyspark
print(f"PySpark version: {pyspark.__version__}")

# Should be 3.5.0 or higher
if pyspark.__version__ < "3.5.0":
    raise RuntimeError("PySpark 3.5.0+ required for UDTF support")
```

## Getting Help

If you encounter issues not covered here:

1. **Check the logs**: Look for error messages in Spark logs or console output
2. **Verify credentials**: Ensure CDF credentials are correct and have proper permissions
3. **Test with simple queries**: Start with basic queries before adding complex filters or joins
4. **Review the documentation**: Check the [Generation](./generation.md) and [Registration](./registration.md) guides
5. **Review the Technical Plan**: See the Technical Plan document for detailed architecture and implementation details

## Next Steps

After successfully using UDTFs in your Spark cluster:

1. **Production Deployment**: Consider how to deploy UDTFs to production Spark clusters
2. **Dependency Management**: Set up proper dependency management for Spark worker nodes
3. **Configuration Management**: Use secure configuration management for credentials in production

For more information, see:
- [Installation](./installation.md)
- [Generation](./generation.md)
- [Registration](./registration.md)
- Technical Plan: CDF Databricks Integration (UDTF-Based)


