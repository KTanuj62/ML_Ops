---------------------------------------------------------------------
-- PHASE 7: AUTOMATION (STREAMS + TASKS)
---------------------------------------------------------------------
-- Fully automated pipeline:
-- Task 1: Load data from stage (scheduled)
-- Task 2: Run full pipeline when new data arrives (stream-triggered)

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE MLOPS_PROD_DB;

-- ============================================
-- 1. CLEANUP (if re-running)
-- ============================================
ALTER TASK IF EXISTS RAW.TRAINING_PIPELINE_TASK SUSPEND;
ALTER TASK IF EXISTS RAW.DATA_LOAD_TASK SUSPEND;
DROP TASK IF EXISTS RAW.TRAINING_PIPELINE_TASK;
DROP TASK IF EXISTS RAW.DATA_LOAD_TASK;
DROP STREAM IF EXISTS RAW.RAW_SALES_STREAM;

-- ============================================
-- 2. STREAM ON RAW_SALES TABLE
-- ============================================
USE SCHEMA RAW;

CREATE OR REPLACE STREAM RAW_SALES_STREAM ON TABLE RAW_SALES
    APPEND_ONLY = TRUE;

-- ============================================
-- 3. MASTER PIPELINE PROCEDURE
-- ============================================
-- Combines: Features → Train → Compare → Monitor
CREATE OR REPLACE PROCEDURE RAW.RUN_TRAINING_PIPELINE()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    feature_result VARCHAR;
    train_result VARCHAR;
    compare_result VARCHAR;
    drift_result VARCHAR;
BEGIN
    -- Step 1: Compute features
    CALL FEATURES.COMPUTE_FEATURES();
    feature_result := 'Features computed';
    
    -- Step 2: Train model
    CALL MODELS.TRAIN_PROPHET_MODEL();
    train_result := 'Model trained';
    
    -- Step 3: Compare and promote
    CALL MODELS.COMPARE_AND_PROMOTE();
    compare_result := 'Comparison complete';
    
    -- Step 4: Check drift
    CALL MONITORING.DETECT_DATA_DRIFT();
    drift_result := 'Drift checked';
    
    -- Log pipeline run
    INSERT INTO MONITORING.PIPELINE_RUNS (PHASE, STATUS, RECORDS_PROCESSED)
    VALUES ('FULL_PIPELINE', 'SUCCESS', (SELECT COUNT(*) FROM FEATURES.FEATURE_STORE));
    
    RETURN '✅ PIPELINE COMPLETE\n' ||
           '1. ' || feature_result || '\n' ||
           '2. ' || train_result || '\n' ||
           '3. ' || compare_result || '\n' ||
           '4. ' || drift_result;
END;
$$;

-- ============================================
-- 4. PIPELINE RUN LOG TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS MONITORING.PIPELINE_RUN_LOG (
    RUN_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    NEW_ROWS INT,
    TRIGGER_TYPE VARCHAR(50)
);

-- ============================================
-- 5. TASK 1: DATA LOAD (Scheduled every 5 min)
-- ============================================
-- Both tasks must be in same schema for AFTER relationship
USE SCHEMA RAW;

CREATE OR REPLACE TASK DATA_LOAD_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
AS
    COPY INTO RAW_SALES (
        MATERIAL_GROUP, MATERIAL_DESCRIPTION, SOLD_STATE, SUPPLY_LOCATION,
        MONTH, FY, NET_SALES, SOURCE_FILE
    )
    FROM (SELECT $1, $2, $3, $4, $5, $6, $7, METADATA$FILENAME FROM @DATA_STAGE)
    FILE_FORMAT = CSV_FORMAT
    ON_ERROR = 'CONTINUE';

-- ============================================
-- 6. TASK 2: TRAINING PIPELINE (After data load, if stream has data)
-- ============================================
-- Must be in same schema as DATA_LOAD_TASK

CREATE OR REPLACE TASK TRAINING_PIPELINE_TASK
    WAREHOUSE = COMPUTE_WH
    AFTER DATA_LOAD_TASK
WHEN
    SYSTEM$STREAM_HAS_DATA('RAW_SALES_STREAM')
AS
    CALL RAW.RUN_TRAINING_PIPELINE();

-- ============================================
-- 7. ENABLE TASKS (child first, then parent)
-- ============================================
ALTER TASK TRAINING_PIPELINE_TASK RESUME;
ALTER TASK DATA_LOAD_TASK RESUME;

-- ============================================
-- 8. VERIFY SETUP
-- ============================================
SHOW STREAMS IN SCHEMA RAW;
SHOW TASKS IN DATABASE MLOPS_PROD_DB;

-- View task status
-- Task status can be viewed with: SHOW TASKS IN SCHEMA RAW;

-- ============================================
-- 9. MANUAL TRIGGER (for testing)
-- ============================================
-- If you want to test without waiting:
-- CALL MODELS.RUN_TRAINING_PIPELINE();

-- ============================================
-- AUTOMATION FLOW:
-- 
-- Upload CSV → @DATA_STAGE
--      ↓
-- DATA_LOAD_TASK (every 5 min)
--      ↓
-- COPY INTO RAW_SALES
--      ↓
-- RAW_SALES_STREAM detects new rows
--      ↓
-- TRAINING_PIPELINE_TASK (runs after DATA_LOAD_TASK, if stream has data)
--      ↓
-- RUN_TRAINING_PIPELINE:
--   1. Compute Features
--   2. Train Prophet Model (CANDIDATE)
--   3. Compare vs PRODUCTION
--   4. Promote if better
--   5. Check Data Drift
--      ↓
-- DONE!
-- ============================================
