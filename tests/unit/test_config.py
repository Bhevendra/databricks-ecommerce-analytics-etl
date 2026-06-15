from common.config import DATASETS, CATALOG, BRONZE_RAW_SCHEMA


def test_config_variables():
    assert CATALOG.startswith('ecommerce')
    assert BRONZE_RAW_SCHEMA == 'bronze_raw'
    assert set(DATASETS.keys()) == {'customers', 'sales_orders', 'sales'}
