SELECT
  MAX(load_timestamp) AS latest_load_timestamp
FROM ecommerce_prod.gold.fact_sales;
