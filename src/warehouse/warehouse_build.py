import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, month, year, quarter, to_date, expr

from common.config import CATALOG, WAREHOUSE_SCHEMA, GOLD_SCHEMA
from common.delta_utils import write_delta


def build_dim_customers(spark: SparkSession):
    customers = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.customer_order_summary")
    return customers.select(
        col('customer_id'),
        col('customer_name'),
        col('order_count'),
        col('total_spend')
    )


def build_dim_date(spark: SparkSession):
    orders = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_order_details")
    return (
        orders.select(to_date(col('order_timestamp')).alias('full_date'))
              .distinct()
              .withColumn('date_key', expr("cast(date_format(full_date, 'yyyyMMdd') as int)"))
              .withColumn('day_num', dayofmonth(col('full_date')))
              .withColumn('month_num', month(col('full_date')))
              .withColumn('year_num', year(col('full_date')))
              .withColumn('quarter_num', quarter(col('full_date')))
    )


def build_fact_orders_item(spark: SparkSession):
    orders = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_order_details")
    return orders.select(
        col('order_number').alias('order_id'),
        col('customer_id'),
        col('product_id'),
        col('order_timestamp'),
        col('quantity'),
        col('unit_price'),
        col('line_total')
    )


def load_warehouse_tables(spark: SparkSession):
    write_delta(build_dim_customers(spark), f"/tmp/etl_ecommerce/warehouse/dim_customers", f"{CATALOG}.{WAREHOUSE_SCHEMA}.dim_customers")
    write_delta(build_dim_date(spark), f"/tmp/etl_ecommerce/warehouse/dim_date", f"{CATALOG}.{WAREHOUSE_SCHEMA}.dim_date")
    write_delta(build_fact_orders_item(spark), f"/tmp/etl_ecommerce/warehouse/fact_orders_item", f"{CATALOG}.{WAREHOUSE_SCHEMA}.fact_orders_item")


if __name__ == '__main__':
    spark = SparkSession.builder.appName('warehouse-build').getOrCreate()
    load_warehouse_tables(spark)
    spark.stop()
