# Testing Guidance - Schema Mismatch Fix

## Overview

This document provides step-by-step instructions for testing the schema mismatch fix that prevents UDTFs from returning 1 column instead of the expected 33/35 columns.

## What Was Fixed

1. **Enhanced Exception Handling**: Added try-catch wrapper around the outer exception handler to ensure it never creates a malformed RecordBatch
2. **Schema Validation**: Added validation after creating debug tables to ensure they have the correct number of columns
3. **RecordBatch Validation**: Added validation for each RecordBatch before yielding to ensure correct schema
4. **Fallback Safety**: If the exception handler itself fails, fall back to `_empty_record_batch()` which is guaranteed to have the correct schema

## Installation Steps

### 1. Install New Wheel Files

In Databricks, install both wheel files:

```python
# Install pygen-spark (with all fixes)
%pip install ./pygen-spark/dist/cognite_pygen_spark-0.0.0-py3-none-any.whl --force-reinstall

# Install cognite-databricks
%pip install ./cognite-databricks/dist/cognite_databricks-0.0.0-py3-none-any.whl --force-reinstall
```

Or if uploading to DBFS:

```python
# Upload both wheels
dbutils.fs.cp("file:///path/to/pygen-spark/dist/cognite_pygen_spark-0.0.0-py3-none-any.whl", "dbfs:/FileStore/wheels/")
dbutils.fs.cp("file:///path/to/cognite-databricks/dist/cognite_databricks-0.0.0-py3-none-any.whl", "dbfs:/FileStore/wheels/")

# Install
%pip install /dbfs/FileStore/wheels/cognite_pygen_spark-0.0.0-py3-none-any.whl --force-reinstall
%pip install /dbfs/FileStore/wheels/cognite_databricks-0.0.0-py3-none-any.whl --force-reinstall
```

### 2. Verify Installation

```python
import cognite.pygen_spark
import cognite.databricks

print(f"pygen-spark version: {cognite.pygen_spark.__version__}")
print(f"cognite-databricks version: {cognite.databricks.__version__}")
```

### 3. Delete Existing Functions

```python
# Delete all functions in the schema
from databricks.sdk import WorkspaceClient

workspace_client = WorkspaceClient()
catalog = "f0connectortest"
schema = "sailboat_sailboat_v1"

# List all functions in the schema
try:
    functions = workspace_client.functions.list(catalog_name=catalog, schema_name=schema)
    function_names = [f.full_name for f in functions]
    
    print(f"Found {len(function_names)} functions to delete:")
    for name in function_names:
        print(f"  - {name}")
    
    # Delete each function
    for name in function_names:
        try:
            workspace_client.functions.delete(name)
            print(f"✓ Deleted: {name}")
        except Exception as e:
            print(f"✗ Failed to delete {name}: {e}")
    
    print(f"\n✓ Deleted {len(function_names)} function(s)")
except Exception as e:
    print(f"Error listing/deleting functions: {e}")
```

### 4. Regenerate UDTFs

```python
from pathlib import Path
from cognite.client import CogniteClient
from cognite.pygen_spark import SparkUDTFGenerator

client = CogniteClient()
generator = SparkUDTFGenerator(
    client=client,
    output_dir=Path("./generated_udtfs"),
    data_model=("sailboat", "sailboat", "v1"),
    debug=True  # CRITICAL: Must be True to include debug columns
)

result = generator.generate_udtfs()
print(f"Generated files: {list(result.generated_files.keys())}")
```

### 5. Verify Generated File Has Fix

```python
from pathlib import Path

# Check the generated file
udtf_file = Path("./generated_udtfs/catalog_registered/cognite_databricks/small_boat_udtf.py")
if udtf_file.exists():
    content = udtf_file.read_text()
    
    # Check for the fixes
    has_validation = "CRITICAL: Validate the created table" in content
    has_critical_catch = "except Exception as critical_error:" in content
    has_batch_validation = "CRITICAL: RecordBatch has" in content
    
    print(f"File exists: {udtf_file}")
    print(f"Has table validation: {has_validation}")
    print(f"Has critical error catch: {has_critical_catch}")
    print(f"Has batch validation: {has_batch_validation}")
    
    if has_validation and has_critical_catch and has_batch_validation:
        print("✓ All fixes present in generated file")
    else:
        print("✗ Some fixes missing - regeneration may be needed")
else:
    print(f"✗ File not found: {udtf_file}")
```

### 6. Re-register UDTFs

```python
from cognite.pygen_spark.registration import register_udtfs
from pathlib import Path

registration_result = register_udtfs(
    catalog="f0connectortest",
    schema="sailboat_sailboat_v1",
    udtf_dir=Path("./generated_udtfs/catalog_registered"),
    secret_scope="cdf_sailboat_sailboat",
    debug=True
)

print(f"Registered: {len(registration_result.registered_udtfs)} UDTF(s)")
for udtf in registration_result.registered_udtfs:
    print(f"  ✓ {udtf.udtf_name}")
```

## Testing Steps

### Test 1: Minimal Debug Query

Test with a minimal query to see debug information:

```sql
SELECT 
    _debug_error,
    _debug_schema_info,
    _debug_column_count,
    _debug_column_names,
    _debug_auth_status,
    _debug_api_status
FROM f0connectortest.sailboat_sailboat_v1.small_boat_udtf(
    client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project => SECRET('cdf_sailboat_sailboat', 'project')
)
LIMIT 1;
```

**Expected Result**: Should return 1 row with all debug columns populated. No schema mismatch error.

### Test 2: Full Query with Data Columns

Test with a full query including data columns:

```sql
SELECT 
    -- Data columns
    name,
    tags,
    files,
    space,
    external_id,
    
    -- Debug columns
    _debug_auth_status,
    _debug_api_status,
    _debug_api_items_count,
    _debug_execution_mode,
    _debug_rows_yielded,
    _debug_error,
    _debug_schema_info,
    _debug_column_count,
    _debug_column_names
FROM f0connectortest.sailboat_sailboat_v1.small_boat_udtf(
    client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project => SECRET('cdf_sailboat_sailboat', 'project'),
    name => NULL,
    description => NULL
)
LIMIT 5;
```

**Expected Result**: Should return up to 5 rows with all data and debug columns. No schema mismatch error.

### Test 3: Error Scenario Test

Test with invalid credentials to verify error handling:

```sql
SELECT 
    _debug_error,
    _debug_auth_status,
    _debug_schema_info,
    _debug_column_count
FROM f0connectortest.sailboat_sailboat_v1.small_boat_udtf(
    client_id => 'invalid',
    client_secret => 'invalid',
    tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project => SECRET('cdf_sailboat_sailboat', 'project')
)
LIMIT 1;
```

**Expected Result**: Should return 1 row with error information in debug columns. No schema mismatch error. All columns should be present.

## Success Criteria

✅ **Test 1 passes**: Debug query returns 1 row with all debug columns  
✅ **Test 2 passes**: Full query returns data with all columns  
✅ **Test 3 passes**: Error scenario returns error info with all columns  
✅ **No schema mismatch errors**: All queries complete without `UDTF_RETURN_SCHEMA_MISMATCH` errors  
✅ **Correct column count**: All queries return the expected number of columns (35: 24 data + 2 metadata + 9 debug)

## Troubleshooting

### If you still get "Expected: 33, Actual: 1"

1. **Verify the generated file has the fixes**:
   - Check for `CRITICAL: Validate the created table` in the generated file
   - Check for `except Exception as critical_error:` in the generated file

2. **Verify the function was re-registered**:
   - Check the registration timestamp
   - Try dropping and re-registering again

3. **Check if there's a caching issue**:
   - Restart the Databricks cluster or detach/reattach to compute
   - Wait a few minutes for cache to clear

4. **Verify the schema**:
   ```python
   spark.sql("DESCRIBE FUNCTION EXTENDED f0connectortest.sailboat_sailboat_v1.small_boat_udtf_internal").show(truncate=False, n=200)
   ```
   - Should show 35 output columns (24 data + 2 metadata + 9 debug)

### If debug columns are missing

1. **Verify `debug=True` was used during generation**:
   ```python
   # Check the generator was called with debug=True
   generator = SparkUDTFGenerator(..., debug=True)
   ```

2. **Verify `debug=True` was used during registration**:
   ```python
   register_udtfs(..., debug=True)
   ```

3. **Check the generated file**:
   - Should have `{% if debug %}` sections
   - Should include all 9 debug columns in schema

## Additional Notes

- **Serverless SQL**: Since Serverless SQL doesn't provide access to worker logs, the debug columns are essential for troubleshooting
- **Schema Validation**: The new validation ensures that even if an exception occurs, we always return the correct schema
- **Fallback Safety**: If the exception handler itself fails, we fall back to `_empty_record_batch()` which is guaranteed to have the correct schema

## Files Changed

- `pygen-spark/cognite/pygen_spark/templates/udtf_function.py.jinja`:
  - Added try-catch wrapper around outer exception handler
  - Added schema validation after creating debug tables
  - Added RecordBatch validation before yielding
  - Added fallback to `_empty_record_batch()` if exception handler fails

## Version Information

- **pygen-spark**: 0.0.0 (with schema mismatch fix)
- **cognite-databricks**: 0.0.0
- **Build Date**: Check wheel file timestamps
