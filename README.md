# UCI-252 - Databricks Retail Lakehouse Pipeline

End-to-end retail Lakehouse data engineering implementation using Databricks, PySpark, Delta Lake, Auto Loader, Lakeflow Pipelines, Lakeflow Jobs, Unity Catalog, Databricks SQL, and GitHub.

## Architecture

```text
CSV source files
      |
      v
Unity Catalog Volume (landing)
      |
      v
Auto Loader
      |
      v
Bronze Delta streaming tables
      |
      v
Silver cleansed / deduplicated tables
      |
      +--------------------+
      |                    |
      v                    v
SCD Type 2 Customer     Gold dimensions
dimension              + fact_sales
      |                    |
      +---------+----------+
                |
                v
      Gold sales aggregations
                |
                v
         Databricks SQL
        dashboard queries
```

## Repository Structure

```text
data/                  Sample retail CSV files
src/pipeline/          Lakeflow pipeline definitions
src/notebooks/         Validation notebook
sql/                   Unity Catalog setup, analytics, validation
resources/             Declarative Automation Bundle resources
docs/                  Architecture, data model, acceptance checklist
scripts/               Helper scripts
tests/                 Local sample-data checks
databricks.yml         Databricks bundle configuration
```

## Main Data Assets

### Bronze
- `retail_lakehouse.bronze.customers`
- `retail_lakehouse.bronze.products`
- `retail_lakehouse.bronze.orders`
- `retail_lakehouse.bronze.order_items`

### Silver
- `retail_lakehouse.silver.customers`
- `retail_lakehouse.silver.products`
- `retail_lakehouse.silver.orders`
- `retail_lakehouse.silver.order_items`

### Gold
- `retail_lakehouse.gold.dim_customer` - SCD Type 2
- `retail_lakehouse.gold.dim_product`
- `retail_lakehouse.gold.dim_date`
- `retail_lakehouse.gold.fact_sales`
- `retail_lakehouse.gold.daily_sales_summary`
- `retail_lakehouse.gold.product_sales_summary`

## Quick Start

1. Run `sql/00_setup_unity_catalog.sql` in a Databricks SQL editor.
2. Upload the CSVs from `data/` into the matching folders under:
   `/Volumes/retail_lakehouse/landing/raw/`
3. Authenticate the Databricks CLI.
4. Validate and deploy the bundle:

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run retail_lakehouse_job -t dev
```

5. Run `sql/20_validation.sql`.
6. Use `sql/30_analytics.sql` for the dashboard.

> Runtime acceptance validation requires a Databricks workspace. The repository itself contains the complete code and deployment configuration.

## Jira

**UCI-252 - Build End-to-End Retail Lakehouse Data Pipeline using Databricks, Delta Lake, Auto Loader and Lakeflow**
