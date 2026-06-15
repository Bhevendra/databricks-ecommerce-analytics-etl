from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

from common.config import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA
from common.delta_utils import merge_delta_table

PRODUCT_SCHEMA = StructType([
    StructField('curr', StringType(), True),
    StructField('id', StringType(), True),
    StructField('name', StringType(), True),
    StructField('price', StringType(), True),
    StructField('qty', StringType(), True),
    StructField('unit', StringType(), True),
])


def transform_sales(df):
    return (
        df.withColumn('product_struct', from_json(col('product'), PRODUCT_SCHEMA))
          .withColumn('product_id', col('product_struct.id'))
          .withColumn('product_price', col('product_struct.price').cast('double'))
          .withColumn('product_qty', col('product_struct.qty').cast('integer'))
          .drop('product_struct')
          .select(
              'customer_id', 'customer_name', 'product_name', 'order_date',
              'product_category', 'product_id', 'product_price', 'product_qty',
              'unit', 'total_price'
          )
    )


def load_silver_sales(spark: SparkSession) -> None:
    source = spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.sales")
    df = transform_sales(source)
    merge_delta_table(spark, df, f"{CATALOG}.{SILVER_SCHEMA}.sales", ['customer_id', 'product_id', 'order_date'])


if __name__ == '__main__':
    spark = SparkSession.builder.appName('silver-sales').getOrCreate()
    load_silver_sales(spark)
    spark.stop()
