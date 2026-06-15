import os

BASE_URL = "https://raw.githubusercontent.com/Bhevendra/ML-Datasets/refs/heads/main/retail_data"

DATASETS = {
    "customers": f"{BASE_URL}/customers.csv",
    "sales_orders": f"{BASE_URL}/sales_orders.csv",
    "sales": f"{BASE_URL}/sales.csv",
}

CATALOG = os.getenv("ETL_CATALOG", "ecommerce_dev")
BRONZE_RAW_SCHEMA = os.getenv("BRONZE_RAW_SCHEMA", "bronze_raw")
BRONZE_SCHEMA = os.getenv("BRONZE_SCHEMA", "bronze")
SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")
GOLD_SCHEMA = os.getenv("GOLD_SCHEMA", "gold")
WAREHOUSE_SCHEMA = os.getenv("WAREHOUSE_SCHEMA", "warehouse")

BRONZE_BASE_PATH = os.getenv("BRONZE_BASE_PATH", "dbfs:/tmp/etl_ecommerce/bronze_raw")
SILVER_BASE_PATH = os.getenv("SILVER_BASE_PATH", "dbfs:/tmp/etl_ecommerce/silver")
GOLD_BASE_PATH = os.getenv("GOLD_BASE_PATH", "dbfs:/tmp/etl_ecommerce/gold")
WAREHOUSE_BASE_PATH = os.getenv("WAREHOUSE_BASE_PATH", "dbfs:/tmp/etl_ecommerce/warehouse")
