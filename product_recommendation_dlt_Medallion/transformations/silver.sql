-- ===========================
-- SILVER LAYER
-- ===========================
-- 1) SILVER CUSTOMERS (batch / non-streaming)
CREATE OR REFRESH LIVE TABLE silver_customers
COMMENT "Cleaned Customer dimension (from bronze_customers)"
AS
WITH cleaned AS (
  SELECT
    CAST(CustomerID AS BIGINT) AS CustomerID,
    TRIM(CustomerName) AS CustomerName,
    REGEXP_REPLACE(ContactNumber,'[^0-9]','') AS ContactNumber,
    TRY_CAST(Age AS INT) AS Age,
    TRIM(Gender) AS Gender,
    TRIM(Location) AS Location,
    TRIM(SubscriptionStatus) AS SubscriptionStatus,
    TRIM(PaymentMethod) AS PaymentMethod,
    COALESCE(TRY_CAST(PreviousPurchases AS INT),0) AS PreviousPurchases,
    TRIM(FrequencyOfPurchases) AS FrequencyOfPurchases,
    TRIM(PreferredSeason) AS PreferredSeason,
    CASE WHEN TRY_CAST(AvgReviewRating AS DOUBLE) BETWEEN 0 AND 5
         THEN TRY_CAST(AvgReviewRating AS DOUBLE) ELSE NULL END AS AvgReviewRating,
    current_timestamp() AS ingest_ts
  FROM LIVE.bronze_customers
  WHERE CustomerID IS NOT NULL
),
dedup AS (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY ingest_ts DESC) AS rn
    FROM cleaned
  ) t WHERE rn = 1
)
SELECT * FROM dedup;


-- 2) SILVER PRODUCTS (batch / latest snapshot per ProductID)
CREATE OR REFRESH LIVE TABLE silver_products
COMMENT "Cleaned product snapshot (latest per ProductID)"
AS
WITH cleaned AS (
  SELECT
    CAST(ProductID AS BIGINT) AS ProductID,
    TRIM(ProductName) AS ProductName,
    TRIM(Category) AS Category,
    TRIM(Brand) AS Brand,
    TRIM(Description) AS Description,
    TRIM(AvailableColors) AS AvailableColors,
    TRIM(AvailableSizes) AS AvailableSizes,
    TRY_CAST(MRP AS DOUBLE) AS MRP,
    TRY_CAST(Price AS DOUBLE) AS Price,
    TRY_CAST(DiscountPercent AS DOUBLE) AS DiscountPercent,
    TRY_CAST(Stock AS INT) AS Stock,
    TRY_CAST(Rating AS DOUBLE) AS Rating,
    TRY_CAST(ReviewsCount AS INT) AS ReviewsCount,
    TRIM(IsUpdated) AS IsUpdated,
    TRY_CAST(LastUpdated AS TIMESTAMP) AS LastUpdated,
    current_timestamp() AS ingest_ts
  FROM LIVE.bronze_products
  WHERE ProductID IS NOT NULL
),
dedup AS (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ProductID ORDER BY LastUpdated DESC, ingest_ts DESC) AS rn
    FROM cleaned
  ) t WHERE rn = 1
)
SELECT * FROM dedup;


-- =======================================================================
-- SILVER SALES (BATCH) - reads streaming bronze_sales and performs cleaning,
-- validation and deduplication. Intentionally NON-STREAMING to allow
-- ROW_NUMBER() dedupe and stable downstream ML.
-- =======================================================================

CREATE OR REFRESH LIVE TABLE silver_sales
COMMENT "Clean deduped sales fact table (reads streaming bronze_sales)"
AS
WITH casted AS (
    SELECT
        CAST(OrderID AS STRING) AS OrderID,
        TRY_CAST(CustomerID AS BIGINT) AS CustomerID,
        TRY_CAST(ProductID AS BIGINT) AS ProductID,
        TRIM(InteractionType) AS InteractionType,
        COALESCE(TRY_CAST(Quantity AS INT),1) AS Quantity,
        TRY_CAST(PriceAtPurchase AS DOUBLE) AS PriceAtPurchase,
        TRIM(PaymentMethod) AS PaymentMethod,
        TRIM(SizeSelected) AS SizeSelected,
        TRIM(ColorSelected) AS ColorSelected,
        TRIM(Category) AS Category,
        TRIM(Brand) AS Brand,
        TRIM(DiscountUsed) AS DiscountUsed,
        TRIM(Season) AS Season,
        TRY_TO_TIMESTAMP(Timestamp) AS EventTime,
        current_timestamp() AS ingest_ts
    FROM LIVE.bronze_sales          -- <-- read as table (NOT STREAM())
    WHERE OrderID IS NOT NULL
),

valid AS (
    SELECT *
    FROM casted
    WHERE EventTime IS NOT NULL
      AND Quantity > 0
      AND PriceAtPurchase IS NOT NULL
),

-- in silver.sql: SILVER SALES (batch/non-streaming variant)
-- ... earlier CTEs (casted, valid) remain the same ...

dedup AS (
  -- keep one row per (OrderID, ProductID), keep latest ingest_ts if duplicates for same item
  SELECT * EXCEPT(rn)
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY OrderID, ProductID
             ORDER BY ingest_ts DESC
           ) AS rn
    FROM valid
  ) t
  WHERE rn = 1
)

SELECT * FROM dedup;
