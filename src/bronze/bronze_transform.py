from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace, to_timestamp

from common.config import CATALOG, BRONZE_RAW_SCHEMA, BRONZE_SCHEMA, BRONZE_BASE_PATH
from common.delta_utils import create_database, write_delta
from common.validation import validate_schema

CUSTOMER_COLUMNS = [
    'customer_id', 'tax_id', 'tax_code', 'customer_name', 'state', 'city',
    'postcode', 'street', 'number', 'unit', 'region', 'district', 'lon',
    'lat', 'ship_to_address', 'valid_from', 'valid_to', 'units_purchased',
    'loyalty_segment', 'data_source', 'ingest_ts'
]

SALES_COLUMNS = [
    'customer_id', 'customer_name', 'product_name', 'order_date',
    'product_category', 'product', 'total_price', 'data_source', 'ingest_ts'
]

SALES_ORDERS_COLUMNS = [
    'clicked_items', 'customer_id', 'customer_name', 'number_of_line_items',
    'order_datetime', 'order_number', 'ordered_products', 'promo_info',
    'data_source', 'ingest_ts'
]


def clean_customers(df):
    return (
        df.select(*[col(c) for c in CUSTOMER_COLUMNS if c in df.columns])
          .withColumn('customer_name', trim(col('customer_name')))
          .withColumn('postcode', regexp_replace(trim(col('postcode')), r'\.?0$', ''))
          .withColumn('number', regexp_replace(trim(col('number')), r'\.?0$', ''))
    )


def clean_sales(df):
    return (
        df.select(*[col(c) for c in SALES_COLUMNS if c in df.columns])
          .withColumn('order_date', to_timestamp('order_date', 'yyyy-MM-dd'))
          .withColumn('total_price', col('total_price').cast('double'))
    )


def clean_sales_orders(df):
    return (
        df.select(*[col(c) for c in SALES_ORDERS_COLUMNS if c in df.columns])
          .withColumn('order_datetime', to_timestamp(col('order_datetime')))
          .withColumn('number_of_line_items', col('number_of_line_items').cast('int'))
    )


def load_bronze_tables(spark: SparkSession) -> None:
    create_database(spark, f"{CATALOG}.{BRONZE_SCHEMA}")

    customers_raw = spark.read.table(f"{CATALOG}.{BRONZE_RAW_SCHEMA}.customers")
    sales_raw = spark.read.table(f"{CATALOG}.{BRONZE_RAW_SCHEMA}.sales")
    sales_orders_raw = spark.read.table(f"{CATALOG}.{BRONZE_RAW_SCHEMA}.sales_orders")

    validate_schema(customers_raw, CUSTOMER_COLUMNS[:-2])
    validate_schema(sales_raw, SALES_COLUMNS[:-2])
    validate_schema(sales_orders_raw, SALES_ORDERS_COLUMNS[:-2])

    customers = clean_customers(customers_raw)
    sales = clean_sales(sales_raw)
    sales_orders = clean_sales_orders(sales_orders_raw)

    write_delta(customers, f"{BRONZE_BASE_PATH}/customers", f"{CATALOG}.{BRONZE_SCHEMA}.customers")
    write_delta(sales, f"{BRONZE_BASE_PATH}/sales", f"{CATALOG}.{BRONZE_SCHEMA}.sales")
    write_delta(sales_orders, f"{BRONZE_BASE_PATH}/sales_orders", f"{CATALOG}.{BRONZE_SCHEMA}.sales_orders")


if __name__ == '__main__':
    spark = SparkSession.builder.appName('bronze-transform').getOrCreate()
    load_bronze_tables(spark)
    spark.stop()
