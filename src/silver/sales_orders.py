from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode_outer, from_unixtime
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, DoubleType, IntegerType

from common.config import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA
from common.delta_utils import merge_delta_table

ORDERED_PRODUCTS_SCHEMA = ArrayType(
    StructType([
        StructField('curr', StringType(), True),
        StructField('id', StringType(), True),
        StructField('name', StringType(), True),
        StructField('price', StringType(), True),
        StructField('promotion_info', StringType(), True),
        StructField('qty', StringType(), True),
        StructField('unit', StringType(), True),
    ])
)

PROMO_SCHEMA = ArrayType(
    StructType([
        StructField('promo_disc', DoubleType(), True),
        StructField('promo_id', StringType(), True),
        StructField('promo_item', StringType(), True),
        StructField('promo_qty', StringType(), True),
    ])
)

CLICKED_ITEMS_SCHEMA = ArrayType(ArrayType(StringType()))


def transform_sales_orders(df):
    orders = (
        df.withColumn('order_timestamp', from_unixtime(col('order_datetime')).cast('timestamp'))
          .select('order_number', 'customer_id', 'customer_name', 'number_of_line_items', 'order_timestamp')
    )

    order_products = (
        df.withColumn('ordered_products_json', from_json(col('ordered_products'), ORDERED_PRODUCTS_SCHEMA))
          .withColumn('ordered_product', explode_outer(col('ordered_products_json')))
          .select(
              'order_number', 'customer_id', 'customer_name',
              col('ordered_product.id').alias('product_id'),
              col('ordered_product.name').alias('product_name'),
              col('ordered_product.price').cast('double').alias('unit_price'),
              col('ordered_product.qty').cast('integer').alias('quantity'),
              col('ordered_product.curr').alias('currency'),
              col('ordered_product.unit').alias('unit')
          )
    )

    promotions = (
        df.withColumn('promo_json', from_json(col('promo_info'), PROMO_SCHEMA))
          .withColumn('promo_row', explode_outer(col('promo_json')))
          .select(
              'order_number', 'customer_id',
              col('promo_row.promo_id').alias('promo_id'),
              col('promo_row.promo_item').alias('promo_product_id'),
              col('promo_row.promo_disc').alias('discount'),
              col('promo_row.promo_qty').cast('integer').alias('promo_quantity')
          )
          .filter(col('promo_id').isNotNull())
    )

    clicked_items = (
        df.withColumn('clicked_items_json', from_json(col('clicked_items'), CLICKED_ITEMS_SCHEMA))
          .withColumn('clicked_item', explode_outer(col('clicked_items_json')))
          .select(
              'order_number', 'customer_id',
              col('clicked_item').getItem(0).alias('product_id'),
              col('clicked_item').getItem(1).cast('integer').alias('score')
          )
          .filter(col('product_id').isNotNull())
    )

    return orders, order_products, promotions, clicked_items


def load_silver_sales_orders(spark: SparkSession) -> None:
    source = spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.sales_orders")
    orders, order_products, promotions, clicked_items = transform_sales_orders(source)
    merge_delta_table(spark, orders, f"{CATALOG}.{SILVER_SCHEMA}.sales_orders", ['order_number'])
    merge_delta_table(spark, order_products, f"{CATALOG}.{SILVER_SCHEMA}.order_products", ['order_number', 'product_id'])
    merge_delta_table(spark, promotions, f"{CATALOG}.{SILVER_SCHEMA}.promotions", ['order_number', 'promo_id'])
    merge_delta_table(spark, clicked_items, f"{CATALOG}.{SILVER_SCHEMA}.clicked_items", ['order_number', 'product_id'])


if __name__ == '__main__':
    spark = SparkSession.builder.appName('silver-sales-orders').getOrCreate()
    load_silver_sales_orders(spark)
    spark.stop()
