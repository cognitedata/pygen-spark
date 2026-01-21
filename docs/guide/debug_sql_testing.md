# Debug SQL Testing Guide

This guide explains how to test UDTFs with debug logging enabled to diagnose property extraction issues.

## Prerequisites

1. UDTFs must be generated with `debug=True` when calling `generate_udtf_notebook()` or `register_udtfs()`
2. The latest wheel files must be installed in your Databricks environment

## Basic Debug Query

The simplest way to test a UDTF with debug logging is to query it with a limit:

```sql
SELECT * FROM TABLE(
  f0connectortest.sailboat_sailboat_v1.small_boat_udtf(
    SECRET('cognite', 'client_id'),
    SECRET('cognite', 'client_secret'),
    SECRET('cognite', 'tenant_id'),
    SECRET('cognite', 'cdf_cluster'),
    SECRET('cognite', 'project')
  )
) LIMIT 5;
```

## Viewing Debug Output

### 1. Driver Logs (Recommended)

Debug output is written to both `sys.stderr` and `print()` statements, which appear in:
- **Databricks Notebook**: Output cell
- **Databricks SQL**: Query profile → Driver logs
- **Spark UI**: Executor logs

### 2. Query Profile Analysis

1. Run your SQL query
2. Click on the query in the query history
3. Navigate to **Query Profile**
4. Expand **Driver** logs
5. Look for lines prefixed with `[UDTF]`

### 3. Debug Columns

When `debug=True`, the UDTF includes debug columns in the result:

```sql
SELECT 
  name,
  description,
  space,
  external_id,
  _debug_auth_status,
  _debug_api_status,
  _debug_api_items_count,
  _debug_execution_mode,
  _debug_rows_yielded,
  _debug_error
FROM TABLE(
  f0connectortest.sailboat_sailboat_v1.small_boat_udtf(
    SECRET('cognite', 'client_id'),
    SECRET('cognite', 'client_secret'),
    SECRET('cognite', 'tenant_id'),
    SECRET('cognite', 'cdf_cluster'),
    SECRET('cognite', 'project')
  )
) LIMIT 10;
```

## Key Debug Information

### Response Structure Analysis

When `debug=True`, the UDTF logs the first item structure from the API response:

```
[UDTF] First item structure:
{
  "space": "sailboat",
  "externalId": "boat::257679870",
  "sources": {
    "boat/v1": {
      "name": "...",
      "description": "..."
    }
  },
  "properties": {
    "sailboat": {
      "boat/v1": {
        "name": "...",
        "description": "..."
      }
    }
  }
}
```

### View Key Lookup

The UDTF logs which view key it's looking for:

```
[UDTF] Looking for view_key: 'boat/v1'
[UDTF] Sources keys: ['boat/v1']
[UDTF] View data in sources: {...}
```

### Property Extraction Status

Check the debug columns:
- `_debug_api_status`: Should be `200` for success
- `_debug_api_items_count`: Number of items returned from API
- `_debug_error`: Should be `null` if no errors occurred

## Troubleshooting Queries

### 1. Check if Properties are Null

```sql
SELECT 
  COUNT(*) as total_rows,
  COUNT(name) as non_null_names,
  COUNT(description) as non_null_descriptions,
  _debug_api_items_count,
  _debug_api_status
FROM TABLE(
  f0connectortest.sailboat_sailboat_v1.small_boat_udtf(
    SECRET('cognite', 'client_id'),
    SECRET('cognite', 'client_secret'),
    SECRET('cognite', 'tenant_id'),
    SECRET('cognite', 'cdf_cluster'),
    SECRET('cognite', 'project')
  )
)
GROUP BY _debug_api_items_count, _debug_api_status;
```

### 2. Inspect First Item Structure

Look for the `[UDTF] First item structure:` log entry in driver logs. This shows:
- Whether `sources` or `properties` structure is used
- The exact view key format
- Available property names

### 3. Verify View Key Format

The view key should match: `{view.external_id}/{view.version}`

Check the logs for:
```
[UDTF] Looking for view_key: 'boat/v1'
[UDTF] Sources keys: ['boat/v1']  -- Should contain the view_key
```

If the view key doesn't match, check:
- View external_id and version in your data model
- Whether the view was generated correctly

### 4. Check API Request Payload

When `debug=True`, the full request payload is logged:

```
[UDTF] Request payload:
{
  "sources": [
    {
      "source": {
        "type": "view",
        "space": "sailboat",
        "externalId": "boat",
        "version": "v1"
      }
    }
  ],
  "instanceType": "node",
  "limit": 1000,
  "includeTyping": false
}
```

Verify:
- `type: "view"` is present
- `space`, `externalId`, and `version` are correct
- `instanceType` matches the view type (node/edge)

## Example: Full Debug Query

```sql
-- Query with all debug information visible
SELECT 
  -- Data columns
  name,
  description,
  space,
  external_id,
  
  -- Debug columns
  _debug_auth_status,
  _debug_api_status,
  _debug_api_items_count,
  _debug_execution_mode,
  _debug_rows_yielded,
  _debug_error
  
FROM TABLE(
  f0connectortest.sailboat_sailboat_v1.small_boat_udtf(
    SECRET('cognite', 'client_id'),
    SECRET('cognite', 'client_secret'),
    SECRET('cognite', 'tenant_id'),
    SECRET('cognite', 'cdf_cluster'),
    SECRET('cognite', 'project')
  )
) 
WHERE _debug_error IS NOT NULL  -- Filter to see only errors
LIMIT 10;
```

## Common Issues and Solutions

### Issue: All properties are NULL

**Check:**
1. Driver logs for `[UDTF] First item structure:` - verify the structure
2. Driver logs for `[UDTF] Sources keys:` or `[UDTF] Properties top-level keys:`
3. Whether the view key matches what's in the response

**Solution:**
- If view key doesn't match, regenerate UDTFs with correct view information
- If structure is different than expected, the code will try both `sources` and `properties` structures automatically

### Issue: API returns 200 but 0 items

**Check:**
- `_debug_api_items_count` - should match the number of instances in CDF
- Request payload in logs - verify filters are correct

**Solution:**
- Check if filters are too restrictive
- Verify the view has data in CDF

### Issue: Authentication errors

**Check:**
- `_debug_auth_status` - should be `SUCCESS`
- `_debug_error` - may contain error details

**Solution:**
- Verify secret scope and keys are correct
- Check OAuth credentials in Databricks secrets

## Serverless SQL Logs

For Databricks Serverless SQL, logs may appear in:
1. **Query Profile** → Driver logs (most reliable)
2. **Compute logs** (if accessible)
3. **File logs** at `/tmp/` or `/dbfs/tmp/` (if file logging is enabled)

Note: Serverless SQL worker logs are separate from driver logs and may not be immediately accessible.

## Next Steps

After reviewing debug output:
1. Check the `[UDTF] First item structure:` log to see the actual API response format
2. Verify the view key matches what's in the response
3. If properties are still NULL, check whether the structure uses `sources` or `properties`
4. The code now handles both structures automatically, but the debug logs will show which one is being used
