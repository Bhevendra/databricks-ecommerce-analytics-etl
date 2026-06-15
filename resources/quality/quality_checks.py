def validate_primary_key(df, column):
    duplicates = (
        df.groupBy(column)
          .count()
          .filter("count > 1")
    )

    if duplicates.count() > 0:
        raise Exception(
            f"Duplicate records found in {column}"
        )


def validate_nulls(df, column):
    null_count = df.filter(
        col(column).isNull()
    ).count()

    if null_count > 0:
        raise Exception(
            f"Null values detected in {column}"
        )
