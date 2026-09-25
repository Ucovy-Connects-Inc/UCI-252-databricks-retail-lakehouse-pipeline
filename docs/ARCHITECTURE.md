# Architecture

The project follows Medallion Architecture.

- **Landing**: source CSVs in a Unity Catalog volume.
- **Bronze**: Auto Loader incrementally ingests raw records and adds ingestion metadata.
- **Silver**: PySpark cleanses, standardizes, deduplicates and validates records.
- **Gold**: SCD2 customer dimension, product/date dimensions, fact sales, and sales aggregates.
- **Orchestration**: Lakeflow Job refreshes the Lakeflow Pipeline and then runs validation.
- **Governance**: Unity Catalog organizes schemas, tables and volumes and provides lineage in Databricks.
- **Analytics**: Databricks SQL queries support sales, customer, product and revenue reporting.
