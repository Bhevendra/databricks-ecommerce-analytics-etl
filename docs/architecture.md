# ETL Ecommerce Analytics Architecture

## Overview

This proof of concept showcases a complete Data Engineering pipeline for Databricks using the Medallion Architecture. It includes:

- Ingestion of remote retail CSV datasets
- Bronze cleansing and raw table creation
- Silver transformation and feature extraction
- Gold fact modeling and aggregated analytics
- Warehouse dimensional modeling and reporting tables
- Data quality validation, security-aware schema design, and CI/CD deployment

## Pipeline stages

1. Ingestion: `src/ingestion/ingest_retail_data.py`
2. Bronze: `src/bronze/bronze_transform.py`
3. Silver: `src/silver/customers.py`, `src/silver/sales.py`, `src/silver/sales_orders.py`
4. Data quality: `src/common/data_quality.py`
5. Gold: `src/gold/gold_transform.py`
6. Warehouse: `src/warehouse/warehouse_build.py`

## Security and deployment

- Databricks job cluster definition: `resources/clusters/job_cluster.yml`
- Job orchestration: `resources/jobs/master_job.yml`
- Environment targets: `environments/dev.yml`, `environments/test.yml`, `environments/prod.yml`
- CI/CD workflows: `.github/workflows/deploy-dev.yml`, `.github/workflows/deploy-test.yml`, `.github/workflows/deploy-prod.yml`

## Data quality

The pipeline performs schema validation, null checks, and uniqueness checks across bronze, silver, and gold tables.

## How to run

1. Install dependencies: `pip install -r requirements.txt`
2. Run the pipeline locally via Databricks local Spark or Databricks cluster:
   - `./run_pipeline.sh`
3. Run unit tests:
   - `pytest tests/unit`
