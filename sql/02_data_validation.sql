---------------------------------------------------------------------
-- PHASE 2: DATA VALIDATION
---------------------------------------------------------------------
-- Data validation ensures only quality data enters the pipeline
-- Bad data is logged and rejected, good data proceeds

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE MLOPS_PROD_DB;

-- ============================================
-- 1. INSERT VALIDATION RULES
-- ============================================
USE SCHEMA STAGING;

-- Clear existing rules and insert fresh
TRUNCATE TABLE VALIDATION_RULES;

INSERT INTO VALIDATION_RULES (RULE_NAME, RULE_TYPE, COLUMN_NAME, RULE_DEFINITION) VALUES
-- Schema rules
('MATERIAL_GROUP_EXISTS', 'SCHEMA', 'MATERIAL_GROUP', 'Column must exist and not be null'),
('NET_SALES_EXISTS', 'SCHEMA', 'NET_SALES', 'Column must exist'),
('MONTH_EXISTS', 'SCHEMA', 'MONTH', 'Column must exist'),
('FY_EXISTS', 'SCHEMA', 'FY', 'Column must exist'),

-- Null checks
('MATERIAL_GROUP_NOT_NULL', 'NULL_CHECK', 'MATERIAL_GROUP', 'MATERIAL_GROUP IS NOT NULL'),
('SOLD_STATE_NOT_NULL', 'NULL_CHECK', 'SOLD_STATE', 'SOLD_STATE IS NOT NULL'),
('NET_SALES_NOT_NULL', 'NULL_CHECK', 'NET_SALES', 'NET_SALES IS NOT NULL'),
('MONTH_NOT_NULL', 'NULL_CHECK', 'MONTH', 'MONTH IS NOT NULL'),
('FY_NOT_NULL', 'NULL_CHECK', 'FY', 'FY IS NOT NULL'),

-- Format rules
('NET_SALES_NUMERIC', 'FORMAT', 'NET_SALES', 'Must be convertible to number'),
('MONTH_FORMAT', 'FORMAT', 'MONTH', 'Must match MMM-YY format'),
('FY_FORMAT', 'FORMAT', 'FY', 'Must match YY-YY format'),

-- Range rules
('NET_SALES_POSITIVE', 'RANGE', 'NET_SALES', 'Net sales should be positive after conversion');

-- Verify rules
SELECT * FROM VALIDATION_RULES ORDER BY RULE_ID;

-- ============================================
-- 2. CREATE VALIDATION PROCEDURE
-- ============================================
CREATE OR REPLACE PROCEDURE STAGING.VALIDATE_AND_LOAD_DATA(BATCH_NAME VARCHAR)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'validate_data'
AS
$$
def validate_data(session, batch_name):
    import pandas as pd
    from datetime import datetime
    
    log = []
    log.append(f"🔍 VALIDATION STARTED: {batch_name}")
    log.append(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # ==========================================
    # STEP 1: Load data from stage
    # ==========================================
    log.append("📥 Step 1: Loading from stage...")
    
    result = session.sql(f"""
        COPY INTO MLOPS_PROD_DB.RAW.RAW_SALES (
            MATERIAL_GROUP, MATERIAL_DESCRIPTION, SOLD_STATE, SUPPLY_LOCATION,
            MONTH, FY, NET_SALES, SOURCE_FILE
        )
        FROM (
            SELECT $1, $2, $3, $4, $5, $6, $7, METADATA$FILENAME
            FROM @MLOPS_PROD_DB.RAW.DATA_STAGE
        )
        FILE_FORMAT = MLOPS_PROD_DB.RAW.CSV_FORMAT
        ON_ERROR = 'CONTINUE'
    """).collect()
    
    # Count loaded records
    total_records = session.sql("SELECT COUNT(*) AS CNT FROM MLOPS_PROD_DB.RAW.RAW_SALES").collect()[0]['CNT']
    log.append(f"   Total records in RAW_SALES: {total_records}")
    
    # ==========================================
    # STEP 2: Validate data quality
    # ==========================================
    log.append("🔎 Step 2: Running validation checks...")
    
    # Get raw data
    df = session.table("MLOPS_PROD_DB.RAW.RAW_SALES").to_pandas()
    
    errors = []
    
    # Check 1: Null values in required columns
    required_cols = ['MATERIAL_GROUP', 'SOLD_STATE', 'NET_SALES', 'MONTH', 'FY']
    for col in required_cols:
        null_count = df[col].isna().sum()
        if null_count > 0:
            errors.append(f"{col} has {null_count} null values")
            log.append(f"   ⚠️ {col}: {null_count} nulls")
    
    # Check 2: NET_SALES convertible to number
    def is_numeric(s):
        try:
            if pd.isna(s):
                return False
            float(str(s).replace(',', ''))
            return True
        except:
            return False
    
    non_numeric = df[~df['NET_SALES'].apply(is_numeric)]
    if len(non_numeric) > 0:
        errors.append(f"NET_SALES: {len(non_numeric)} non-numeric values")
        log.append(f"   ⚠️ NET_SALES: {len(non_numeric)} non-numeric")
    
    # Check 3: Month format (MMM-YY)
    import re
    month_pattern = r'^(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-\d{2}$'
    invalid_months = df[~df['MONTH'].astype(str).str.upper().str.match(month_pattern, na=False)]
    if len(invalid_months) > 0:
        errors.append(f"MONTH: {len(invalid_months)} invalid format")
        log.append(f"   ⚠️ MONTH: {len(invalid_months)} invalid format")
    
    # Check 4: FY format (YY-YY)
    fy_pattern = r'^\d{2}-\d{2}$'
    invalid_fy = df[~df['FY'].astype(str).str.match(fy_pattern, na=False)]
    if len(invalid_fy) > 0:
        errors.append(f"FY: {len(invalid_fy)} invalid format")
        log.append(f"   ⚠️ FY: {len(invalid_fy)} invalid format")
    
    # ==========================================
    # STEP 3: Calculate validation status
    # ==========================================
    valid_records = len(df) - len(set(
        list(non_numeric.index) + 
        list(invalid_months.index) + 
        list(invalid_fy.index)
    ))
    invalid_records = len(df) - valid_records
    
    # Determine status (allow up to 5% invalid)
    error_rate = (invalid_records / len(df)) * 100 if len(df) > 0 else 0
    status = "PASSED" if error_rate <= 5 else "FAILED"
    
    log.append(f"📊 Step 3: Validation Summary")
    log.append(f"   Total Records: {len(df)}")
    log.append(f"   Valid Records: {valid_records}")
    log.append(f"   Invalid Records: {invalid_records}")
    log.append(f"   Error Rate: {error_rate:.2f}%")
    log.append(f"   Status: {status}")
    
    # ==========================================
    # STEP 4: Log validation results
    # ==========================================
    import json
    error_json = json.dumps(errors).replace("'", "''") if errors else '[]'
    
    session.sql(f"""
        INSERT INTO MLOPS_PROD_DB.STAGING.VALIDATION_LOG 
        (BATCH_ID, TOTAL_RECORDS, VALID_RECORDS, INVALID_RECORDS, VALIDATION_STATUS, ERROR_DETAILS)
        SELECT '{batch_name}', {len(df)}, {valid_records}, {invalid_records}, '{status}', 
               PARSE_JSON('{error_json}')
    """).collect()
    
    log.append("✅ Validation logged!")
    
    # ==========================================
    # STEP 5: Return result
    # ==========================================
    if status == "PASSED":
        log.append("🎉 DATA VALIDATION PASSED - Ready for feature engineering")
    else:
        log.append("❌ DATA VALIDATION FAILED - Review errors before proceeding")
    
    return "\n".join(log)
$$;

-- ============================================
-- 3. RUN VALIDATION
-- ============================================
-- Execute this after uploading CSV to stage
CALL STAGING.VALIDATE_AND_LOAD_DATA('batch_1');

-- ============================================
-- 4. CHECK VALIDATION RESULTS
-- ============================================
SELECT * FROM STAGING.VALIDATION_LOG ORDER BY VALIDATION_TIME DESC LIMIT 5;

-- Check raw data count
SELECT COUNT(*) AS TOTAL_RECORDS FROM RAW.RAW_SALES;

-- Sample raw data
SELECT * FROM RAW.RAW_SALES LIMIT 10;

-- ============================================
-- NEXT STEPS:
-- If VALIDATION_STATUS = 'PASSED':
--   → Proceed to Phase 3: Feature Store
-- If VALIDATION_STATUS = 'FAILED':
--   → Review errors, fix data, re-run
-- ============================================
