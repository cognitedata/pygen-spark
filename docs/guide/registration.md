# Registration

## Overview

After generating UDTF code, you need to register the UDTFs in your Spark session. Registration makes the UDTFs available for use in SQL queries.

## Register a Single UDTF

### Step 1: Load the Generated UDTF Class

Load the generated UDTF class from the file:

```python
from pathlib import Path
import sys

# Add the generated code directory to Python path
udtf_dir = Path("./generated_udtfs")
sys.path.insert(0, str(udtf_dir))

# Import the generated UDTF class
from cognite_udtfs.SmallBoat_udtf import SmallBoatUDTF
```

### Step 2: Register with Spark

Register the UDTF using PySpark's `udtf()` function:

```python
from pyspark.sql.functions import udtf

# Wrap the class with udtf()
smallboat_udtf = udtf(SmallBoatUDTF)

# Register in Spark session
spark.udtf.register("smallboat_udtf", smallboat_udtf)

print("✓ UDTF registered: smallboat_udtf")
```

## Register Multiple UDTFs

To register all generated UDTFs:

```python
from pathlib import Path
import sys
from pyspark.sql.functions import udtf

# Add generated code directory to path
udtf_dir = Path("./generated_udtfs")
sys.path.insert(0, str(udtf_dir))

# Import all UDTF classes
from cognite_udtfs.SmallBoat_udtf import SmallBoatUDTF
from cognite_udtfs.LargeBoat_udtf import LargeBoatUDTF

# Register all UDTFs
registered = {}
for udtf_class, func_name in [
    (SmallBoatUDTF, "smallboat_udtf"),
    (LargeBoatUDTF, "largeboat_udtf"),
]:
    udtf_wrapped = udtf(udtf_class)
    spark.udtf.register(func_name, udtf_wrapped)
    registered[func_name] = udtf_class.__name__

print(f"✓ Registered {len(registered)} UDTF(s):")
for func_name, class_name in registered.items():
    print(f"  - {func_name} ({class_name})")
```

## Verify Registration

Verify that UDTFs are registered:

```python
# List registered UDTFs
registered_udtfs = spark.catalog.listFunctions()
udtf_names = [f.name for f in registered_udtfs if "udtf" in f.name.lower()]

print(f"Registered UDTFs: {udtf_names}")
```

## Alternative: Dynamic Loading

For a more dynamic approach, you can load and register UDTFs programmatically:

```python
from pathlib import Path
import importlib.util
from pyspark.sql.functions import udtf

def register_udtf_from_file(file_path: Path, function_name: str):
    """Load and register a UDTF from a file."""
    # Load the module
    spec = importlib.util.spec_from_file_location("udtf_module", file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    
    # Find the UDTF class (class ending with "UDTF")
    udtf_class = None
    for name in dir(module):
        obj = getattr(module, name)
        if (isinstance(obj, type) and 
            name.endswith("UDTF") and 
            hasattr(obj, "eval") and 
            hasattr(obj, "analyze")):
            udtf_class = obj
            break
    
    if udtf_class is None:
        raise ValueError(f"No UDTF class found in {file_path}")
    
    # Register
    udtf_wrapped = udtf(udtf_class)
    spark.udtf.register(function_name, udtf_wrapped)
    return function_name

# Register from file
udtf_file = Path("./generated_udtfs/cognite_udtfs/SmallBoat_udtf.py")
register_udtf_from_file(udtf_file, "smallboat_udtf")
```

## Important Notes

1. **Session-Scoped**: Registered UDTFs are only available in the current Spark session. They are not persisted across sessions.

2. **Dependencies**: Ensure `cognite-sdk` is installed on all Spark worker nodes. The generated UDTF code requires it.

3. **Analyze Method**: The generated UDTFs include an `analyze()` method required for PySpark Connect. This method is automatically used by Spark.

4. **Function Names**: Use lowercase with underscores for function names (e.g., `smallboat_udtf`). This follows SQL naming conventions.

## Next Steps

After registration, you can start [Querying](./querying.md) your UDTFs.

