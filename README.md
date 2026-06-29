# etl-ecommerce-analytics

## Project Overview

This repository contains a complete end-to-end Data Engineering proof of concept built to showcase industry-standard architecture for Databricks, Delta Lake, and modern CI/CD. It ingests live retail data from GitHub CSV files, applies the Medallion Architecture, validates data quality, and defines deployment artifacts for Databricks job orchestration.

## Architecture Summary

The pipeline is structured into these layers:

- **Ingestion**: Raw retail datasets are ingested directly from GitHub CSV sources into the Bronze layer.
- **Bronze**: Raw data is cleansed and persisted as Delta tables for auditability and traceability.
- **Silver**: Business-ready transformations are applied, including JSON parsing, key normalization, and deduplication.
- **Gold**: Analytical fact tables and aggregated summary tables are produced for reporting and BI consumption.
- **Warehouse**: Dimensional models are built to support a data warehouse layer for downstream analytics.
- **Data Quality**: Schema, null, and uniqueness validation is enforced across bronze, silver, and gold.
- **Security & Deployment**: Databricks resources, environment targets, and GitHub Actions are defined to support deployment and environment separation.

## Data Sources

This POC uses the following live datasets:

- `customers.csv`
- `sales_orders.csv`
- `sales.csv`

All datasets are sourced from the GitHub raw content URLs under the `Bhevendra/ML-Datasets` repository.

## Medallion Architecture

### Bronze

The Bronze layer preserves raw and minimally transformed data with audit metadata. It retains the original source schema and adds ingestion metadata to support lineage and debugging.

### Silver

The Silver layer contains business-cleaned, enriched, and normalized data:

- customer names and addresses are standardized
- JSON payloads are parsed into structured columns
- promotional data and clicked items are expanded
- SCD-style merge logic is used to avoid duplicates and support incremental updates

### Gold

The Gold layer produces analytical fact and summary tables for BI and reporting. It joins Silver tables into a consolidated fact model and computes metrics such as order totals and customer spend.

### Warehouse

A dimensional warehouse layer is built on top of Gold to support traditional analytics and reporting:

- `dim_customers`
- `dim_date`
- `fact_orders_item`

## Security Architecture

This project is designed with Databricks best practices in mind:

- **Unity Catalog-ready structure** using catalog and schema separation
- **Environment-specific configurations** for dev/test/prod
- **Cluster and job definitions** stored in `resources/clusters` and `resources/jobs`
- **Permission skeleton** defined under `resources/permissions`

## CI/CD and Deployment

The repository includes GitHub Actions workflows for deployment targets:

- `.github/workflows/deploy-dev.yml`
- `.github/workflows/deploy-test.yml`
- `.github/workflows/deploy-prod.yml`

The `databricks.yml` Asset Bundle config defines the bundle and environment targets for Databricks deployment.

## Project Structure

```
etl-ecommerce-analytics/
├── databricks.yml
├── variables.yml
├── resources/
│   ├── jobs/
│   ├── clusters/
│   ├── permissions/
│   ├── quality/
│   └── pipelines/
├── src/
│   ├── ingestion/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── warehouse/
│   └── common/
├── tests/
│   ├── unit/
│   ├── integration/
│   └── data_quality/
├── monitoring/
├── docs/
├── environments/
│   ├── dev.yml
│   ├── test.yml
│   └── prod.yml
└── .github/
    └── workflows/
```

## Core Components

- `src/ingestion/ingest_retail_data.py`: ingestion layer that reads remote CSVs and writes Bronze Delta tables.
- `src/bronze/bronze_transform.py`: bronze processing and raw table creation.
- `src/silver/customers.py`: customer silver transformation and merge logic.
- `src/silver/sales.py`: sales silver transformation with JSON parsing.
- `src/silver/sales_orders.py`: order and product detail transformation.
- `src/gold/gold_transform.py`: fact table creation for analytics.
- `src/warehouse/warehouse_build.py`: dimensional warehouse model creation.
- `src/common/data_quality.py`: data quality rules and validation checks.
- `src/common/delta_utils.py`: Delta Lake helpers and merge utilities.
- `src/etl_pipeline.py`: end-to-end pipeline orchestration entrypoint.

## How to Run

### Local Development

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Run the pipeline:

```bash
./run_pipeline.sh
```

> Note: a Spark/Delta runtime environment is required for local execution.

### Testing

```bash
pytest tests/unit
```

### Databricks Deployment

Use the GitHub Actions workflows and `databricks.yml` bundle config to deploy to dev, test, and prod targets.

## Data Quality and Observability

The project includes data quality checks for:

- schema consistency
- nullability rules
- uniqueness of business keys

Monitoring assets are stored in the `monitoring/` directory and can be extended with additional SLA and freshness checks.
