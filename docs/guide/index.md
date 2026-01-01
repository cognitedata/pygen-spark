# pygen-spark User Guide

## Introduction

`pygen-spark` generates strongly-typed Python User-Defined Table Functions (UDTFs) from CDF Data Models, enabling you to query CDF data directly from Spark SQL. The generated UDTFs work with any Spark cluster (standalone, YARN, Kubernetes, or local development).

This approach is ideal for:

- **Standalone Spark Clusters**: Deploy UDTFs to standard Spark clusters without Databricks-specific features
- **Development and Testing**: Quickly test UDTFs in local or development environments
- **Production Deployments**: Use UDTFs in production Spark clusters with configuration file-based credential management
- **Flexible Credential Management**: Use TOML/YAML configuration files for secure credential handling

## Overview

This documentation covers the complete workflow for using pygen-spark:

1. **[Installation](./installation.md)**: Set up dependencies and verify your environment
2. **[Generation](./generation.md)**: Generate UDTF code from CDF Data Models
3. **[Registration](./registration.md)**: Register UDTFs in your Spark session
4. **[Querying](./querying.md)**: Query UDTFs using SQL with credential parameters
5. **[Filtering](./filtering.md)**: Filter data using WHERE clauses with predicate pushdown
6. **[Joining](./joining.md)**: Join data from different UDTFs based on `external_id` and `space`
7. **[Time Series](./time_series.md)**: Work with time series UDTFs (if available)
8. **[Troubleshooting](./troubleshooting.md)**: Common issues and solutions

## Quick Links

### Examples

- [Basic Generation](../examples/basic_generation.ipynb): Generate UDTFs from a CDF Data Model
- [Registration](../examples/registration.ipynb): Register and query UDTFs
- [Querying Data](../examples/querying_data.ipynb): Query single/multiple UDTFs, named vs positional parameters
- [Filtering Queries](../examples/filtering_queries.ipynb): Equality, range, NULL handling, multiple conditions
- [Joining UDTFs](../examples/joining_udtfs.ipynb): Joins on external_id, space+external_id, CROSS JOIN LATERAL

### Related Documentation

- [pygen](https://github.com/cognitedata/pygen): Base code generation library for CDF Data Models
- [cognite-databricks](https://github.com/cognitedata/cognite-databricks): Helper SDK for Databricks-specific features (Unity Catalog, Secret Manager)
- Technical Plan: CDF Databricks Integration (UDTF-Based)

