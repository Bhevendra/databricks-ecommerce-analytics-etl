import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pyspark.sql import SparkSession
from ingestion.ingest_retail_data import run_ingestion
from bronze.bronze_transform import load_bronze_tables
from silver.customers import load_silver_customers
from silver.sales import load_silver_sales
from silver.sales_orders import load_silver_sales_orders
from common.data_quality import run_data_quality
from gold.gold_transform import load_gold_tables
from warehouse.warehouse_build import load_warehouse_tables

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


def main() -> None:
    spark = SparkSession.builder.appName('etl-ecommerce-analytics-poc').getOrCreate()
    logging.info('Starting ingestion stage')
    run_ingestion(spark)

    logging.info('Starting bronze transformation stage')
    load_bronze_tables(spark)

    logging.info('Starting silver customers stage')
    load_silver_customers(spark)

    logging.info('Starting silver sales stage')
    load_silver_sales(spark)

    logging.info('Starting silver sales orders stage')
    load_silver_sales_orders(spark)

    logging.info('Running data quality checks')
    run_data_quality(spark)

    logging.info('Starting gold build stage')
    load_gold_tables(spark)

    logging.info('Starting warehouse model stage')
    load_warehouse_tables(spark)

    spark.stop()
    logging.info('ETL pipeline completed successfully')


if __name__ == '__main__':
    main()
