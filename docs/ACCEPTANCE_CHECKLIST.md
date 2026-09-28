# UCI-252 Acceptance Checklist

Runtime items must not be marked complete without execution evidence.

- [ ] Databricks environment configured.
- [ ] Unity Catalog schemas and volume created.
- [x] Retail sample datasets prepared.
- [x] Auto Loader Bronze ingestion implemented.
- [x] Bronze ingestion metadata implemented.
- [x] Silver PySpark transformations implemented.
- [x] Data cleansing, deduplication and null handling implemented.
- [x] Data-quality expectations implemented.
- [x] Customer SCD Type 2 code implemented.
- [x] `dim_customer`, `dim_product`, `dim_date` implemented.
- [x] `fact_sales` implemented.
- [x] Gold sales/revenue aggregations implemented.
- [x] Lakeflow Pipeline resource implemented.
- [x] Lakeflow Job orchestration resource implemented.
- [x] Databricks SQL analytics queries included.
- [ ] Databricks SQL dashboard created in workspace.
- [ ] Runtime pipeline validation completed.
- [x] Architecture and data model documentation included.
- [ ] Feature branch committed and PR raised.
