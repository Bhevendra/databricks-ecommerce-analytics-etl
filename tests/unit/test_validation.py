from common.validation import validate_schema


class DummyDataFrame:
    def __init__(self, columns):
        self.columns = columns


def test_validate_schema_pass():
    df = DummyDataFrame(['customer_id', 'customer_name', 'state'])
    validate_schema(df, ['customer_id', 'customer_name'])


def test_validate_schema_missing_column():
    df = DummyDataFrame(['customer_id', 'state'])
    try:
        validate_schema(df, ['customer_id', 'customer_name'])
    except ValueError as exc:
        assert 'missing columns' in str(exc)
    else:
        raise AssertionError('Expected ValueError for missing columns')
