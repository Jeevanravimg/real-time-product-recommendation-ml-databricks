# New Pipeline 2025-12-10 10:52

This folder contains all source code for the 'New Pipeline 2025-12-10 10:52' Databricks DLT pipeline.

## Project Structure

- `explorations`: Ad-hoc notebooks for data exploration.
- `transformations`: Dataset definitions and transformations for the pipeline.

## Medallion Architecture: Bronze, Silver, and Gold

This pipeline implements the medallion architecture, organizing data into three layers:

- **Bronze**: Raw, ingested data from source systems. 
  - Streaming and incremental data ingestion using Auto Loader.
  - Schema enforcement and basic cleansing.
  - All product events and user interactions are captured for recommendation use cases.

- **Silver**: Cleaned and enriched data.
  - Deduplication and filtering of invalid records.
  - Business logic applied, including joining user, product, and event datasets.
  - Incremental processing ensures only new/changed data is transformed.
  - Feature engineering for product recommendation (e.g., user-product interaction matrices).

- **Gold**: Aggregated, business-ready data for analytics and reporting.
  - Final product recommendation tables generated using collaborative filtering or other ML techniques.
  - Aggregations for dashboards and BI tools.
  - Optimized for consumption, with incremental updates reflecting latest recommendations.

Each layer builds on the previous, ensuring data quality and traceability throughout the pipeline.

## Getting Started

1. Clone this repository and open it in Databricks.
2. Go to the `transformations` folder to view and edit dataset definitions.
   - Each dataset is defined in a separate file.
   - See "sample_users_dec_10_1052.sql" for a syntax example.
   - Reference: [Databricks SQL Syntax](https://docs.databricks.com/ldp/developer/sql-ref).
3. Use `Run file` to preview a single transformation.
4. Use `Run pipeline` to execute all transformations in the pipeline.
5. Use `+ Add` in the file browser to add new dataset definitions.
6. Use `Schedule` to automate pipeline runs.

## Version Control

This project is now tracked in Git. Commit and push changes to keep your pipeline source code versioned.

## Resources

- [Databricks DLT Documentation](https://docs.databricks.com/ldp)
- [DLT-meta Project](https://github.com/databrickslabs/dlt-meta) (for advanced pipeline generation)

---