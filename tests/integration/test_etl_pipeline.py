import pytest


@pytest.mark.skip(reason='Integration tests require a Spark environment with delta and Databricks configurations')
def test_etl_pipeline_smoke():
    assert True
