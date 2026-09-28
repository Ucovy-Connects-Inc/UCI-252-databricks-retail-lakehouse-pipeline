CREATE CATALOG IF NOT EXISTS retail_lakehouse;

CREATE SCHEMA IF NOT EXISTS retail_lakehouse.landing
COMMENT 'Landing area for retail source files';

CREATE SCHEMA IF NOT EXISTS retail_lakehouse.bronze
COMMENT 'Raw incrementally ingested Delta tables';

CREATE SCHEMA IF NOT EXISTS retail_lakehouse.silver
COMMENT 'Cleansed, deduplicated and validated datasets';

CREATE SCHEMA IF NOT EXISTS retail_lakehouse.gold
COMMENT 'Analytics-ready fact, dimension and aggregate datasets';

CREATE SCHEMA IF NOT EXISTS retail_lakehouse.pipeline_meta
COMMENT 'Default schema for Lakeflow pipeline metadata';

CREATE VOLUME IF NOT EXISTS retail_lakehouse.landing.raw
COMMENT 'Landing volume for retail CSV source files';

SHOW SCHEMAS IN CATALOG retail_lakehouse;
SHOW VOLUMES IN SCHEMA retail_lakehouse.landing;
