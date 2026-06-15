import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

from common.config import DATASETS, CATALOG, BRONZE_RAW_SCHEMA, BRONZE_BASE_PATH
from common.delta_utils import create_database, write_delta
from common.validation import validate_schema


def read_remote_csv(spark: SparkSession, url: str):
    return spark.read.option("header", True).option("inferSchema", True).csv(url)


def ingest_dataset(spark: SparkSession, dataset_name: str, dataset_url: str):
    raw_df = (
        read_remote_csv(spark, dataset_url)
        .withColumn("data_source", lit(dataset_url))
        .withColumn("ingest_ts", current_timestamp())
    )

    expected_columns = {
        "customers": [
            "customer_id",
            "tax_id",
            "tax_code",
            "customer_name",
            "state",
            "city",
            "postcode",
            "street",
            "number",
            "unit",
            "region",
            "district",
            "lon",
            "lat",
            "ship_to_address",
            "valid_from",
            "valid_to",
            "units_purchased",
            "loyalty_segment",
        ],
        "sales_orders": [
            "clicked_items",
            "customer_id",
            "customer_name",
            "number_of_line_items",
            "order_datetime",
            "order_number",
            "ordered_products",
            "promo_info",
        ],
        "sales": [
            "customer_id",
            "customer_name",
            "product_name",
            "order_date",
            "product_category",
            "product",
            "total_price",
        ],
    }

    validate_schema(raw_df, expected_columns[dataset_name])
    raw_db = f"{CATALOG}.{BRONZE_RAW_SCHEMA}"
    create_database(spark, raw_db)

    path = f"{BRONZE_BASE_PATH}/{dataset_name}"
    table_name = f"{raw_db}.{dataset_name}"
    write_delta(raw_df, path, table_name)
    print(f"Ingested {dataset_name} into {table_name}")


def run_ingestion(spark: SparkSession) -> None:
    for dataset_name, dataset_url in DATASETS.items():
        ingest_dataset(spark, dataset_name, dataset_url)


def main() -> None:
    spark = SparkSession.builder.appName("etl-ecommerce-ingestion").getOrCreate()
    run_ingestion(spark)
    spark.stop()


if __name__ == "__main__":
    main()
