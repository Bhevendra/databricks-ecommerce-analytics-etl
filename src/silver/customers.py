from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, trim

from common.config import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA, SILVER_BASE_PATH
from common.delta_utils import merge_delta_table

EXPECTED_COLUMNS = [
    'customer_id', 'customer_name', 'tax_id', 'city', 'state', 'postcode'
]


def transform_customers(df):
    return (
        df.withColumn('name_parts', split(col('customer_name'), ','))
          .withColumn('last_name', trim(col('name_parts').getItem(0)))
          .withColumn('first_name', trim(col('name_parts').getItem(1)))
          .drop('name_parts')
          .select(
              'customer_id', 'first_name', 'last_name', 'tax_id', 'state',
              'city', 'postcode', 'region', 'district', 'lon', 'lat',
              'ship_to_address', 'units_purchased', 'loyalty_segment'
          )
    )


def load_silver_customers(spark: SparkSession) -> None:
    source = spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.customers")
    df = transform_customers(source)
    merge_delta_table(spark, df, f"{CATALOG}.{SILVER_SCHEMA}.customers", ['customer_id'])


if __name__ == '__main__':
    spark = SparkSession.builder.appName('silver-customers').getOrCreate()
    load_silver_customers(spark)
    spark.stop()
