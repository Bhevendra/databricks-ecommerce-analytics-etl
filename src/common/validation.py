from functools import reduce
from operator import or_
from typing import Iterable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def validate_not_null(df: DataFrame, columns: Iterable[str]) -> None:
    expressions = [col(column).isNull() for column in columns]
    null_condition = reduce(or_, expressions)
    null_count = df.filter(null_condition).count()
    if null_count > 0:
        raise ValueError(f"Data quality check failed: {null_count} null values found in columns {list(columns)}")


def validate_unique(df: DataFrame, columns: Iterable[str]) -> None:
    duplicate_count = (
        df.groupBy(*columns)
          .count()
          .filter("count > 1")
          .count()
    )
    if duplicate_count > 0:
        raise ValueError(f"Data quality check failed: {duplicate_count} duplicate key group(s) found for columns {list(columns)}")


def validate_schema(df: DataFrame, expected_columns: Iterable[str]) -> None:
    actual_columns = set(df.columns)
    missing_columns = [column for column in expected_columns if column not in actual_columns]
    if missing_columns:
        raise ValueError(f"Data quality check failed: missing columns {missing_columns}")
