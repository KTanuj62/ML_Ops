---------------------------------------------------------------------
-- PHASE 6: MONITORING & DRIFT DETECTION
---------------------------------------------------------------------
-- Track model performance and detect data drift
-- Alert when performance degrades

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE MLOPS_PROD_DB;
USE SCHEMA MONITORING;

-- ============================================
-- 1. DATA DRIFT DETECTION PROCEDURE
-- ============================================
-- Compares current data statistics vs baseline (when model was trained)

CREATE OR REPLACE PROCEDURE MONITORING.DETECT_DATA_DRIFT()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'numpy')
HANDLER = 'detect_drift'
AS
$$
def detect_drift(session):
    import pandas as pd
    import numpy as np
    from datetime import datetime
    
    log = []
    log.append(f"🔍 DATA DRIFT CHECK: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Get current feature statistics
    df = session.table("MLOPS_PROD_DB.FEATURES.FEATURE_STORE").to_pandas()
    
    if len(df) == 0:
        return "❌ No data in FEATURE_STORE"
    
    # Calculate current statistics
    current_stats = {
        'Y_MEAN': df['Y'].mean(),
        'Y_STD': df['Y'].std(),
        'Y_MIN': df['Y'].min(),
        'Y_MAX': df['Y'].max(),
        'RECORD_COUNT': len(df)
    }
    
    log.append(f"📊 Current Stats:")
    log.append(f"   Mean: {current_stats['Y_MEAN']:,.0f}")
    log.append(f"   Std: {current_stats['Y_STD']:,.0f}")
    log.append(f"   Range: {current_stats['Y_MIN']:,.0f} - {current_stats['Y_MAX']:,.0f}")
    
    # Get baseline (from last drift check or use current as baseline)
    baseline_df = session.sql("""
        SELECT BASELINE_VALUE FROM MLOPS_PROD_DB.MONITORING.DRIFT_METRICS
        WHERE METRIC_NAME = 'Y_MEAN'
        ORDER BY CHECK_DATE DESC LIMIT 1
    """).collect()
    
    if not baseline_df:
        # First run - set baseline
        log.append("📌 Setting baseline (first run)")
        for metric, value in current_stats.items():
            session.sql(f"""
                INSERT INTO MLOPS_PROD_DB.MONITORING.DRIFT_METRICS
                (METRIC_NAME, BASELINE_VALUE, CURRENT_VALUE, DRIFT_SCORE, DRIFT_STATUS)
                SELECT '{metric}', {value}, {value}, 0, 'BASELINE'
            """).collect()
        return "\n".join(log) + "\n✅ Baseline established"
    
    # Calculate drift
    baseline_mean = float(baseline_df[0]['BASELINE_VALUE'])
    drift_pct = abs((current_stats['Y_MEAN'] - baseline_mean) / baseline_mean) * 100
    
    # Determine status
    if drift_pct < 10:
        status = 'NORMAL'
    elif drift_pct < 25:
        status = 'WARNING'
    else:
        status = 'CRITICAL'
    
    log.append(f"📈 Drift Analysis:")
    log.append(f"   Baseline Mean: {baseline_mean:,.0f}")
    log.append(f"   Current Mean: {current_stats['Y_MEAN']:,.0f}")
    log.append(f"   Drift: {drift_pct:.2f}%")
    log.append(f"   Status: {status}")
    
    # Log drift metrics
    session.sql(f"""
        INSERT INTO MLOPS_PROD_DB.MONITORING.DRIFT_METRICS
        (METRIC_NAME, BASELINE_VALUE, CURRENT_VALUE, DRIFT_SCORE, DRIFT_STATUS)
        SELECT 'Y_MEAN', {baseline_mean}, {current_stats['Y_MEAN']}, {drift_pct}, '{status}'
    """).collect()
    
    if status == 'CRITICAL':
        log.append("⚠️ ALERT: Significant data drift detected! Consider retraining.")
    elif status == 'WARNING':
        log.append("⚡ Warning: Moderate data drift detected. Monitor closely.")
    else:
        log.append("✅ Data drift within normal range.")
    
    return "\n".join(log)
$$;

-- ============================================
-- 2. MODEL HEALTH CHECK PROCEDURE
-- ============================================
CREATE OR REPLACE PROCEDURE MONITORING.MODEL_HEALTH_CHECK()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    prod_version VARCHAR;
    prod_mae FLOAT;
    prod_date TIMESTAMP_NTZ;
    days_since_training INT;
    health_status VARCHAR;
    message VARCHAR;
BEGIN
    -- Get production model
    SELECT MODEL_VERSION, MAE, TRAINING_DATE 
    INTO :prod_version, :prod_mae, :prod_date
    FROM MODELS.MODEL_REGISTRY
    WHERE STATUS = 'PRODUCTION'
    LIMIT 1;
    
    IF (prod_version IS NULL) THEN
        RETURN '❌ No PRODUCTION model found';
    END IF;
    
    -- Calculate days since training
    days_since_training := DATEDIFF('day', prod_date, CURRENT_TIMESTAMP());
    
    -- Determine health status
    IF (days_since_training > 90) THEN
        health_status := 'STALE';
        message := 'Model is ' || days_since_training || ' days old. Consider retraining.';
    ELSEIF (days_since_training > 30) THEN
        health_status := 'AGING';
        message := 'Model is ' || days_since_training || ' days old.';
    ELSE
        health_status := 'HEALTHY';
        message := 'Model is fresh (' || days_since_training || ' days old).';
    END IF;
    
    -- Log health check
    INSERT INTO MONITORING.PIPELINE_RUNS (PHASE, STATUS, RECORDS_PROCESSED)
    VALUES ('HEALTH_CHECK', :health_status, :days_since_training);
    
    RETURN '🏥 MODEL HEALTH CHECK\n' ||
           'Model: ' || :prod_version || '\n' ||
           'MAE: ' || :prod_mae || '\n' ||
           'Trained: ' || :prod_date || '\n' ||
           'Age: ' || :days_since_training || ' days\n' ||
           'Status: ' || :health_status || '\n' ||
           :message;
END;
$$;

-- ============================================
-- 3. SUMMARY DASHBOARD VIEW
-- ============================================
CREATE OR REPLACE VIEW MONITORING.PIPELINE_SUMMARY AS
SELECT 
    'PRODUCTION_MODEL' AS METRIC,
    (SELECT MODEL_VERSION FROM MODELS.MODEL_REGISTRY WHERE STATUS = 'PRODUCTION' LIMIT 1) AS VALUE
UNION ALL
SELECT 
    'PRODUCTION_MAE',
    (SELECT CAST(MAE AS VARCHAR) FROM MODELS.MODEL_REGISTRY WHERE STATUS = 'PRODUCTION' LIMIT 1)
UNION ALL
SELECT 
    'TOTAL_FEATURES',
    (SELECT CAST(COUNT(*) AS VARCHAR) FROM FEATURES.FEATURE_STORE)
UNION ALL
SELECT 
    'RAW_RECORDS',
    (SELECT CAST(COUNT(*) AS VARCHAR) FROM RAW.RAW_SALES)
UNION ALL
SELECT 
    'LATEST_DRIFT_STATUS',
    (SELECT DRIFT_STATUS FROM MONITORING.DRIFT_METRICS ORDER BY CHECK_DATE DESC LIMIT 1)
UNION ALL
SELECT 
    'MODELS_TRAINED',
    (SELECT CAST(COUNT(*) AS VARCHAR) FROM MODELS.MODEL_REGISTRY);

-- ============================================
-- 4. RUN MONITORING
-- ============================================
-- Detect data drift
CALL MONITORING.DETECT_DATA_DRIFT();

-- Check model health
CALL MONITORING.MODEL_HEALTH_CHECK();

-- View summary
SELECT * FROM MONITORING.PIPELINE_SUMMARY;

-- View drift history
SELECT * FROM MONITORING.DRIFT_METRICS ORDER BY CHECK_DATE DESC LIMIT 10;

-- ============================================
-- NEXT STEPS:
-- → Proceed to Phase 7: Automation
-- ============================================
