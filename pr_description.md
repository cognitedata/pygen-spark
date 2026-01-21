# Release 0.2.0 - Enhanced Error Handling and Direct REST API Calls

This release includes significant improvements to error handling, direct REST API integration, and time series UDTF enhancements.

## Bump

- [ ] Patch
- [x] Minor
- [ ] Skip

## Changelog
### Added

- Added `_error` column to UDTF output schemas for better error visibility in query results
- Direct REST API calls in generated UDTFs (no Cognite SDK dependency at runtime)
- Enhanced error messages with error categories (AUTHENTICATION, CONFIGURATION, NETWORK, UNKNOWN)
- Protobuf parser support for time series datapoints with JSON fallback
- HTTP client module with OAuth2 token caching and retry logic
- Support for distributed limit calculation across multiple time series items

### Changed

- UDTFs now use direct REST API calls instead of Cognite SDK at runtime
- Improved request payload alignment with SDK behavior (ignoreUnknownIds, limit distribution)
- Updated time series UDTF templates to match SDK's retrieve_arrays behavior
- Enhanced error handling with structured error messages in output

### Fixed

- Fixed time series UDTF request payload to match SDK behavior (ignoreUnknownIds: True)
- Fixed limit distribution across multiple time series items (distributes total limit, not per-item)
- Fixed CI/CD compatibility issues (ruff UP038, mypy pyspark imports)
- Fixed dependency resolution for cognite-databricks integration
- Fixed get_file method to handle _session and _catalog suffixes

### Improved

- Removed debug functionality from all UDTF templates for cleaner output
- Improved exception handling specificity throughout codebase
- Enhanced code quality (linting, type checking, formatting)
- Updated documentation to reflect direct REST API approach
- Improved import organization and code style alignment with pygen-main

### Removed

- Removed time_series_datapoints_long_udtf and time_series_datapoints_multi_udtf templates
- Removed all debug-related code and columns from UDTF templates
- Removed Cognite SDK runtime dependency from generated UDTFs
