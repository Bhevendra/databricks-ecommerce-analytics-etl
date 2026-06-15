from pyspark.sql import SparkSession
from common.validation import validate_not_null, validate_unique, validate_schema
from common.config import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA


def quality_bronze(spark: SparkSession) -> None:
    customers = spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.customers")
    sales = spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.sales")
    sales_orders = spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.sales_orders")

    validate_schema(customers, [
        'customer_id', 'customer_name', 'state', 'city'
    ])
    validate_not_null(customers, ['customer_id'])
    validate_unique(customers, ['customer_id'])

    validate_schema(sales, [
        'customer_id', 'product_name', 'order_date', 'product_category'
    ])
    validate_not_null(sales, ['customer_id', 'order_date'])

    validate_schema(sales_orders, [
        'order_number', 'customer_id', 'ordered_products'
    ])
    validate_not_null(sales_orders, ['order_number', 'customer_id'])


def quality_silver(spark: SparkSession) -> None:
    customers = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.customers")
    sales = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sales")
    sales_orders = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sales_orders")
    order_products = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.order_products")

    validate_schema(customers, ['customer_id', 'first_name', 'last_name'])
    validate_not_null(customers, ['customer_id'])
    validate_unique(customers, ['customer_id'])

    validate_schema(sales, ['customer_id', 'product_id', 'order_date', 'total_price'])
    validate_not_null(sales, ['customer_id', 'product_id', 'order_date'])

    validate_schema(sales_orders, ['order_number', 'customer_id', 'order_timestamp'])
    validate_not_null(sales_orders, ['order_number', 'customer_id'])

    validate_schema(order_products, ['order_number', 'product_id', 'quantity', 'unit_price'])
    validate_not_null(order_products, ['order_number', 'product_id'])
    validate_unique(order_products, ['order_number', 'product_id'])


def quality_gold(spark: SparkSession) -> None:
    fact = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_order_details")
    validate_schema(fact, ['order_number', 'product_id', 'customer_id', 'line_total'])
    validate_not_null(fact, ['order_number', 'product_id'])
    validate_unique(fact, ['order_number', 'product_id'])


def run_data_quality(spark: SparkSession) -> None:
    quality_bronze(spark)
    quality_silver(spark)
    quality_gold(spark)
