# Release 0.2.1 - Fix UDTF Template Issues

This release fixes critical issues in UDTF templates that were causing zero-row results and runtime errors.

## Bump

- [x] Patch
- [ ] Minor
- [ ] Skip

## Changelog

### Fixed

- Fixed missing `json` import in `udtf_function.py.jinja` template that caused `NameError: name 'json' is not defined` when processing data model UDTFs with array or relationship properties
- Fixed `time_series_datapoints_detailed_udtf` to properly parse protobuf responses instead of attempting to parse JSON, which was causing zero-row results
- Implemented proper protobuf parsing with JSON fallback for detailed time series UDTF, extracting instanceId, status codes, and symbols from protobuf responses
- Ensured all helper functions have access to `json` module by adding import at the beginning of `eval()` method

### Improved

- Enhanced protobuf parsing logic to extract detailed information (status codes, status symbols, external_id, space) for each datapoint in detailed UDTF
- Improved error handling with proper fallback to JSON parsing when protobuf is unavailable
- Better support for all datapoint types (numeric, string, aggregate) in protobuf responses
