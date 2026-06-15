from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable


def create_database(spark: SparkSession, name: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {name}")


def write_delta(df: DataFrame, path: str, table_name: str, mode: str = "overwrite") -> None:
    df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(path)
    db_name = ".".join(table_name.split(".")[:-1])
    create_database(df.sparkSession, db_name)
    df.sparkSession.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{path}'")


def merge_delta_table(spark: SparkSession, source_df: DataFrame, target_table: str, business_keys: list[str]) -> None:
    if not spark.catalog.tableExists(target_table):
        source_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
        return
    delta_table = DeltaTable.forName(spark, target_table)
    merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in business_keys])
    delta_table.alias("target").merge(
        source_df.alias("source"),
        merge_condition
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
