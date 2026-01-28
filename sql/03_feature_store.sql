---------------------------------------------------------------------
-- PHASE 3: FEATURE STORE
---------------------------------------------------------------------
-- Centralized feature engineering
-- Creates consistent features for training and inference

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE MLOPS_PROD_DB;
USE SCHEMA FEATURES;

-- ============================================
-- 1. CREATE CLEAN DATA TABLE
-- ============================================
CREATE OR REPLACE TABLE FEATURES.CLEAN_SALES AS
SELECT
    UPPER(TRIM(MATERIAL_GROUP)) AS BRAND,
    UPPER(TRIM(MATERIAL_DESCRIPTION)) AS PRODUCT,
    UPPER(TRIM(SOLD_STATE)) AS STATE,
    UPPER(TRIM(SUPPLY_LOCATION)) AS LOCATION,
    -- Parse Month
    CASE UPPER(SPLIT_PART(MONTH, '-', 1))
        WHEN 'JAN' THEN 1  WHEN 'FEB' THEN 2  WHEN 'MAR' THEN 3
        WHEN 'APR' THEN 4  WHEN 'MAY' THEN 5  WHEN 'JUN' THEN 6
        WHEN 'JUL' THEN 7  WHEN 'AUG' THEN 8  WHEN 'SEP' THEN 9
        WHEN 'OCT' THEN 10 WHEN 'NOV' THEN 11 WHEN 'DEC' THEN 12
    END AS MONTH_NUM,
    TRY_TO_NUMBER('20' || SPLIT_PART(MONTH, '-', 2)) AS YEAR,
    TRY_TO_DECIMAL(REPLACE(NET_SALES, ',', ''), 15, 2) AS NET_SALES,
    -- Create Period (YYYY-MM)
    CONCAT(
        CASE 
            WHEN CASE UPPER(SPLIT_PART(MONTH, '-', 1))
                WHEN 'JAN' THEN 1  WHEN 'FEB' THEN 2  WHEN 'MAR' THEN 3
                WHEN 'APR' THEN 4  WHEN 'MAY' THEN 5  WHEN 'JUN' THEN 6
                WHEN 'JUL' THEN 7  WHEN 'AUG' THEN 8  WHEN 'SEP' THEN 9
                WHEN 'OCT' THEN 10 WHEN 'NOV' THEN 11 WHEN 'DEC' THEN 12
            END >= 4 THEN '20' || SPLIT_PART(FY, '-', 1)
            ELSE '20' || SPLIT_PART(FY, '-', 2)
        END, '-', 
        LPAD(CASE UPPER(SPLIT_PART(MONTH, '-', 1))
            WHEN 'JAN' THEN 1  WHEN 'FEB' THEN 2  WHEN 'MAR' THEN 3
            WHEN 'APR' THEN 4  WHEN 'MAY' THEN 5  WHEN 'JUN' THEN 6
            WHEN 'JUL' THEN 7  WHEN 'AUG' THEN 8  WHEN 'SEP' THEN 9
            WHEN 'OCT' THEN 10 WHEN 'NOV' THEN 11 WHEN 'DEC' THEN 12
        END, 2, '0')
    ) AS PERIOD,
    SOURCE_FILE
FROM RAW.RAW_SALES
WHERE MONTH IS NOT NULL AND FY IS NOT NULL
  AND TRY_TO_DECIMAL(REPLACE(NET_SALES, ',', ''), 15, 2) IS NOT NULL;

-- Verify clean data
SELECT COUNT(*) AS CLEAN_RECORDS FROM FEATURES.CLEAN_SALES;
SELECT * FROM FEATURES.CLEAN_SALES LIMIT 10;

-- ============================================
-- 2. CREATE AGGREGATED TABLE
-- ============================================
CREATE OR REPLACE TABLE FEATURES.AGG_SALES AS
SELECT 
    BRAND, 
    STATE, 
    PERIOD,
    SUM(NET_SALES) AS TOTAL_SALES,
    COUNT(*) AS TRANSACTION_COUNT
FROM FEATURES.CLEAN_SALES
GROUP BY BRAND, STATE, PERIOD
ORDER BY BRAND, STATE, PERIOD;

-- Verify aggregated data
SELECT COUNT(*) AS AGG_RECORDS FROM FEATURES.AGG_SALES;

-- ============================================
-- 3. COMPUTE FEATURES FOR PROPHET
-- ============================================
-- Prophet requires: 
--   ds (date column)
--   y (target value)

CREATE OR REPLACE TABLE FEATURES.FEATURE_STORE AS
WITH base AS (
    SELECT 
        BRAND, 
        STATE, 
        PERIOD,
        -- Create date from period (first day of month)
        TO_DATE(PERIOD || '-01', 'YYYY-MM-DD') AS DS,
        TOTAL_SALES AS Y,
        CAST(SUBSTR(PERIOD, 1, 4) AS INT) AS YEAR,
        CAST(SUBSTR(PERIOD, 6, 2) AS INT) AS MONTH,
        CEIL(CAST(SUBSTR(PERIOD, 6, 2) AS INT) / 3.0) AS QUARTER
    FROM FEATURES.AGG_SALES
),
with_lags AS (
    SELECT 
        b.*,
        -- Lag features
        LAG(Y, 1) OVER (PARTITION BY BRAND, STATE ORDER BY DS) AS LAG_1,
        LAG(Y, 2) OVER (PARTITION BY BRAND, STATE ORDER BY DS) AS LAG_2,
        LAG(Y, 3) OVER (PARTITION BY BRAND, STATE ORDER BY DS) AS LAG_3,
        -- Rolling averages
        AVG(Y) OVER (PARTITION BY BRAND, STATE ORDER BY DS ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS ROLLING_AVG_3
    FROM base b
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY BRAND, STATE, DS) AS FEATURE_ID,
    BRAND,
    STATE,
    PERIOD,
    DS,
    Y,
    YEAR,
    MONTH,
    QUARTER,
    LAG_1,
    LAG_2,
    LAG_3,
    ROLLING_AVG_3,
    CURRENT_TIMESTAMP() AS CREATED_AT
FROM with_lags 
WHERE LAG_3 IS NOT NULL;  -- Need at least 3 months history

-- ============================================
-- 4. CREATE FEATURE COMPUTATION PROCEDURE
-- ============================================
CREATE OR REPLACE PROCEDURE FEATURES.COMPUTE_FEATURES()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Refresh clean data
    CREATE OR REPLACE TABLE FEATURES.CLEAN_SALES AS
    SELECT
        UPPER(TRIM(MATERIAL_GROUP)) AS BRAND,
        UPPER(TRIM(MATERIAL_DESCRIPTION)) AS PRODUCT,
        UPPER(TRIM(SOLD_STATE)) AS STATE,
        UPPER(TRIM(SUPPLY_LOCATION)) AS LOCATION,
        CASE UPPER(SPLIT_PART(MONTH, '-', 1))
            WHEN 'JAN' THEN 1  WHEN 'FEB' THEN 2  WHEN 'MAR' THEN 3
            WHEN 'APR' THEN 4  WHEN 'MAY' THEN 5  WHEN 'JUN' THEN 6
            WHEN 'JUL' THEN 7  WHEN 'AUG' THEN 8  WHEN 'SEP' THEN 9
            WHEN 'OCT' THEN 10 WHEN 'NOV' THEN 11 WHEN 'DEC' THEN 12
        END AS MONTH_NUM,
        TRY_TO_NUMBER('20' || SPLIT_PART(MONTH, '-', 2)) AS YEAR,
        TRY_TO_DECIMAL(REPLACE(NET_SALES, ',', ''), 15, 2) AS NET_SALES,
        CONCAT(
            CASE 
                WHEN CASE UPPER(SPLIT_PART(MONTH, '-', 1))
                    WHEN 'JAN' THEN 1  WHEN 'FEB' THEN 2  WHEN 'MAR' THEN 3
                    WHEN 'APR' THEN 4  WHEN 'MAY' THEN 5  WHEN 'JUN' THEN 6
                    WHEN 'JUL' THEN 7  WHEN 'AUG' THEN 8  WHEN 'SEP' THEN 9
                    WHEN 'OCT' THEN 10 WHEN 'NOV' THEN 11 WHEN 'DEC' THEN 12
                END >= 4 THEN '20' || SPLIT_PART(FY, '-', 1)
                ELSE '20' || SPLIT_PART(FY, '-', 2)
            END, '-', 
            LPAD(CASE UPPER(SPLIT_PART(MONTH, '-', 1))
                WHEN 'JAN' THEN 1  WHEN 'FEB' THEN 2  WHEN 'MAR' THEN 3
                WHEN 'APR' THEN 4  WHEN 'MAY' THEN 5  WHEN 'JUN' THEN 6
                WHEN 'JUL' THEN 7  WHEN 'AUG' THEN 8  WHEN 'SEP' THEN 9
                WHEN 'OCT' THEN 10 WHEN 'NOV' THEN 11 WHEN 'DEC' THEN 12
            END, 2, '0')
        ) AS PERIOD,
        SOURCE_FILE
    FROM RAW.RAW_SALES
    WHERE MONTH IS NOT NULL AND FY IS NOT NULL
      AND TRY_TO_DECIMAL(REPLACE(NET_SALES, ',', ''), 15, 2) IS NOT NULL;
    
    -- Refresh aggregate
    CREATE OR REPLACE TABLE FEATURES.AGG_SALES AS
    SELECT BRAND, STATE, PERIOD, SUM(NET_SALES) AS TOTAL_SALES, COUNT(*) AS TRANSACTION_COUNT
    FROM FEATURES.CLEAN_SALES
    GROUP BY BRAND, STATE, PERIOD ORDER BY BRAND, STATE, PERIOD;
    
    -- Refresh features
    CREATE OR REPLACE TABLE FEATURES.FEATURE_STORE AS
    WITH base AS (
        SELECT BRAND, STATE, PERIOD,
               TO_DATE(PERIOD || '-01', 'YYYY-MM-DD') AS DS,
               TOTAL_SALES AS Y,
               CAST(SUBSTR(PERIOD, 1, 4) AS INT) AS YEAR,
               CAST(SUBSTR(PERIOD, 6, 2) AS INT) AS MONTH,
               CEIL(CAST(SUBSTR(PERIOD, 6, 2) AS INT) / 3.0) AS QUARTER
        FROM FEATURES.AGG_SALES
    ),
    with_lags AS (
        SELECT b.*,
               LAG(Y, 1) OVER (PARTITION BY BRAND, STATE ORDER BY DS) AS LAG_1,
               LAG(Y, 2) OVER (PARTITION BY BRAND, STATE ORDER BY DS) AS LAG_2,
               LAG(Y, 3) OVER (PARTITION BY BRAND, STATE ORDER BY DS) AS LAG_3,
               AVG(Y) OVER (PARTITION BY BRAND, STATE ORDER BY DS ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS ROLLING_AVG_3
        FROM base b
    )
    SELECT ROW_NUMBER() OVER (ORDER BY BRAND, STATE, DS) AS FEATURE_ID,
           BRAND, STATE, PERIOD, DS, Y, YEAR, MONTH, QUARTER, LAG_1, LAG_2, LAG_3, ROLLING_AVG_3,
           CURRENT_TIMESTAMP() AS CREATED_AT
    FROM with_lags WHERE LAG_3 IS NOT NULL;
    
    RETURN 'Features computed: ' || (SELECT COUNT(*) FROM FEATURES.FEATURE_STORE) || ' records';
END;
$$;

-- ============================================
-- 5. VERIFY FEATURE STORE
-- ============================================
SELECT COUNT(*) AS FEATURE_COUNT FROM FEATURES.FEATURE_STORE;

-- Prophet-ready data preview
SELECT BRAND, STATE, DS, Y, LAG_1, LAG_2, LAG_3, ROLLING_AVG_3
FROM FEATURES.FEATURE_STORE 
ORDER BY DS
LIMIT 20;

-- Unique Brand-State combinations
SELECT BRAND, STATE, COUNT(*) AS PERIODS
FROM FEATURES.FEATURE_STORE
GROUP BY BRAND, STATE
ORDER BY PERIODS DESC;

-- ============================================
-- NEXT STEPS:
-- → Proceed to Phase 4: Prophet Model Training
-- ============================================
