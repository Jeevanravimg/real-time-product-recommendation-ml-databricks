-- DLT Bronze Layer for Product Recommendation System
-- Load sequence: Customers → Products → Sales

------------------------------------------------------
-- 1. Bronze Customers (Incremental Batch)
------------------------------------------------------
CREATE OR REFRESH LIVE TABLE bronze_customers
COMMENT "Raw customer incremental data from volume"
AS SELECT *
FROM read_files(
  '/Volumes/kusha_solutions/products_recommendation_online_ml/streaming_sales_data/customers/',
  format => 'csv',
  header => 'true'
);


------------------------------------------------------
-- 2. Bronze Products (Incremental Batch)
------------------------------------------------------
CREATE OR REFRESH LIVE TABLE bronze_products
COMMENT "Raw product incremental data from volume"
AS SELECT *
FROM read_files(
  '/Volumes/kusha_solutions/products_recommendation_online_ml/streaming_sales_data/products/',
  format => 'csv',
  header => 'true'
);


------------------------------------------------------
-- 3. Bronze Sales (Streaming)
------------------------------------------------------
CREATE OR REFRESH STREAMING LIVE TABLE bronze_sales
COMMENT "Raw streaming sales events from volume"
AS SELECT *
FROM cloud_files(
  '/Volumes/kusha_solutions/products_recommendation_online_ml/streaming_sales_data/sales/',
  'csv',
  map("header","true","inferSchema","true")
);
