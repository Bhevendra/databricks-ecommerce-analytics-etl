from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

from common.config import CATALOG, SILVER_SCHEMA, GOLD_SCHEMA
from common.delta_utils import write_delta


def build_fact_order_details(spark: SparkSession):
    orders = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sales_orders")
    order_products = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.order_products")
    customers = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.customers")

    fact = (
        order_products.alias('op')
          .join(orders.alias('so'), 'order_number', 'left')
          .join(customers.alias('c'), 'customer_id', 'left')
          .select(
              col('op.order_number'),
              col('op.product_id'),
              col('op.product_name'),
              col('op.quantity'),
              col('op.unit_price'),
              col('op.currency'),
              col('op.unit'),
              col('so.order_timestamp'),
              col('so.customer_id'),
              col('so.customer_name'),
              col('c.first_name'),
              col('c.last_name'),
              col('c.state'),
              col('c.city'),
              expr('op.quantity * op.unit_price').alias('line_total')
          )
    )
    return fact


def build_customer_order_summary(spark: SparkSession):
    fact = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_order_details")
    summary = (
        fact.groupBy('customer_id', 'customer_name')
            .agg(
                expr('count(distinct order_number)').alias('order_count'),
                expr('sum(line_total)').alias('total_spend')
            )
    )
    return summary


def load_gold_tables(spark: SparkSession):
    fact = build_fact_order_details(spark)
    write_delta(fact, f"/tmp/etl_ecommerce/gold/fact_order_details", f"{CATALOG}.{GOLD_SCHEMA}.fact_order_details")
    summary = build_customer_order_summary(spark)
    write_delta(summary, f"/tmp/etl_ecommerce/gold/customer_order_summary", f"{CATALOG}.{GOLD_SCHEMA}.customer_order_summary")


if __name__ == '__main__':
    spark = SparkSession.builder.appName('gold-build').getOrCreate()
    load_gold_tables(spark)
    spark.stop()
