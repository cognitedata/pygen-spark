# Testing Guidance for UDTF Column Fix

## Overview

This document provides guidance for testing the fixes applied to resolve the schema mismatch issue where UDTFs were returning 1 column instead of the expected 30 columns.

## Changes Made

### 1. Fixed Column Extraction Issue
- **Problem**: `arrow_table.columns` returns Column objects, not arrays
- **Fix**: Extract underlying Arrow arrays using `col.chunk(0)` for each column
- **Files Modified**:
  - `templates/udtf_function.py.jinja`
  - `templates/time_series_datapoints_udtf.py.jinja`
  - `templates/time_series_latest_datapoints_udtf.py.jinja`
  - `templates/time_series_datapoints_long_udtf.py.jinja`

### 2. Added Schema Validation
- Added validation in `_json_to_arrow_table()` to ensure correct number of columns
- Added validation in `_arrow_table_to_record_batches()` to verify RecordBatch schemas
- Added validation when combining tables with debug columns

### 3. Enhanced Debug Logging
- Added new debug columns:
  - `_debug_schema_info`: Shows expected vs actual column counts
  - `_debug_column_count`: Actual number of columns in the table
  - `_debug_column_names`: Comma-separated list of column names
- These columns are only available when `debug=True` during UDTF generation

## Testing Steps

### Step 1: Install the New Wheel

```python
# In Databricks notebook or Python environment
%pip install /path/to/cognite_pygen_spark-0.0.0-py3-none-any.whl --force-reinstall
```

Or if using a file path:
```python
%pip install ./dist/cognite_pygen_spark-0.0.0-py3-none-any.whl --force-reinstall
```

### Step 2: Regenerate UDTFs with Debug Mode

```python
from pathlib import Path
from cognite.client import CogniteClient
from cognite.pygen_spark import SparkUDTFGenerator

# Initialize client
client = CogniteClient()

# Create generator with debug=True
generator = SparkUDTFGenerator(
    client=client,
    output_dir=Path("./generated_udtfs"),
    data_model=("sailboat", "sailboat", "v1"),
    debug=True  # CRITICAL: Enable debug mode
)

# Generate UDTFs
result = generator.generate_udtfs()
print(f"Generated {result.total_count} UDTF(s)")
```

### Step 3: Register the UDTF

```python
from cognite.pygen_spark.registration import register_udtfs

# Register with Unity Catalog
registration_result = register_udtfs(
    catalog="f0connectortest",
    schema="sailboat_sailboat_v1",
    udtf_dir=Path("./generated_udtfs/catalog_registered"),
    secret_scope="cdf_sailboat_sailboat",
    debug=True  # Enable debug mode for registration
)

print(f"Registered {len(registration_result.registered_udtfs)} UDTF(s)")
```

### Step 4: Test the UDTF Query

Run your original SQL query:

```sql
SELECT 
    name,
    tags,
    files,
    space,
    external_id,
    -- Debug columns (only visible when debug=True)
    _debug_schema_info,
    _debug_column_count,
    _debug_column_names,
    _debug_auth_status,
    _debug_api_status,
    _debug_api_items_count,
    _debug_execution_mode,
    _debug_rows_yielded,
    _debug_error
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

### Step 5: Verify Results

#### Expected Behavior

1. **No Schema Mismatch Error**: The query should execute without the `UDTF_RETURN_SCHEMA_MISMATCH` error

2. **Correct Column Count**: 
   - Check `_debug_column_count` - should match expected number (28 properties + space + external_id = 30, or 39 with debug columns)
   - Check `_debug_schema_info` - should show `expected=30,actual=30` (or similar)

3. **All Columns Present**:
   - Check `_debug_column_names` - should list all expected column names
   - Verify all data columns (name, tags, files, etc.) are present and populated

4. **Debug Information**:
   - `_debug_auth_status`: Should show "SUCCESS" if authentication worked
   - `_debug_api_status`: Should show HTTP status code (e.g., "200")
   - `_debug_api_items_count`: Should show number of items returned from API
   - `_debug_execution_mode`: Should show "arrow"
   - `_debug_rows_yielded`: Should show number of rows returned
   - `_debug_error`: Should be NULL if no errors occurred

### Step 6: Test Error Scenarios

#### Test with Invalid Credentials

```sql
SELECT * FROM f0connectortest.sailboat_sailboat_v1.small_boat_udtf(
    client_id => 'invalid',
    client_secret => 'invalid',
    tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project => SECRET('cdf_sailboat_sailboat', 'project')
)
LIMIT 1;
```

**Expected**: Should return 1 row with:
- All data columns as NULL
- `_debug_auth_status`: "FAILED"
- `_debug_error`: Contains error message
- `_debug_schema_info`: Shows error state

#### Test with Empty Results

Use a filter that returns no results:

```sql
SELECT * FROM f0connectortest.sailboat_sailboat_v1.small_boat_udtf(
    client_id => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project => SECRET('cdf_sailboat_sailboat', 'project'),
    name => 'NONEXISTENT_NAME_THAT_WILL_NOT_MATCH'
)
LIMIT 1;
```

**Expected**: Should return 1 row with:
- All data columns as NULL
- `_debug_api_items_count`: 0
- `_debug_rows_yielded`: 0
- `_debug_error`: NULL (no error, just no results)

## Troubleshooting

### Issue: Still Getting Schema Mismatch Error

**Possible Causes**:
1. Old UDTF code still registered - regenerate and re-register
2. Cache issues - restart the cluster or clear Unity Catalog cache
3. Wrong UDTF version - verify you're using the catalog_registered version

**Solution**:
```python
# Drop and recreate the UDTF
spark.sql("DROP FUNCTION IF EXISTS f0connectortest.sailboat_sailboat_v1.small_boat_udtf")

# Re-register
register_udtfs(...)
```

### Issue: Debug Columns Not Showing

**Possible Causes**:
1. UDTF was generated without `debug=True`
2. Using wrong UDTF file (session_scoped vs catalog_registered)

**Solution**:
- Regenerate with `debug=True`
- Use `catalog_registered` directory for Unity Catalog registration

### Issue: Column Count Still Wrong

**Check**:
1. Look at `_debug_column_names` to see which columns are present
2. Check `_debug_schema_info` to see expected vs actual
3. Verify the view has the expected number of properties

**Solution**:
- Check the view definition in CDF
- Verify properties_list in the generated code matches view properties
- Check logs for validation errors

## Validation Checklist

- [ ] Wheel file built successfully
- [ ] UDTFs regenerated with `debug=True`
- [ ] UDTFs re-registered in Unity Catalog
- [ ] Query executes without schema mismatch error
- [ ] `_debug_column_count` matches expected count
- [ ] `_debug_schema_info` shows correct expected/actual values
- [ ] All data columns are present and accessible
- [ ] Debug columns provide useful information
- [ ] Error scenarios handled correctly with debug info

## Additional Notes

1. **Debug Mode Overhead**: Debug mode adds 9 additional columns. For production, consider generating without debug mode for better performance.

2. **Logging**: Check Databricks Log4j output for additional debug messages written to stderr.

3. **Performance**: The fixes add minimal overhead - mainly validation checks that should not impact performance significantly.

4. **Backward Compatibility**: UDTFs generated without debug mode will work the same as before, just without the additional debug columns.
