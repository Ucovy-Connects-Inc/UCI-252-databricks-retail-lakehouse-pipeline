## UCI-252 - Databricks Retail Lakehouse Pipeline

### Summary
Implemented an end-to-end retail Lakehouse codebase using Databricks, Delta Lake, Auto Loader, PySpark, Lakeflow Pipelines, Lakeflow Jobs, Unity Catalog and Databricks SQL.

### Changes Included
- Retail sample datasets
- Unity Catalog setup SQL
- Auto Loader Bronze ingestion with metadata
- Silver cleansing, deduplication and data-quality expectations
- Customer SCD Type 2 using Lakeflow AUTO CDC
- Gold dimensions and `fact_sales`
- Daily and product sales aggregations
- Lakeflow Pipeline and Job bundle resources
- Databricks SQL analytics and dashboard specification
- Validation notebook and validation SQL
- Architecture, data-model and acceptance documentation

### Jira
UCI-252

### Note
Runtime deployment and workspace validation require Databricks workspace access.
