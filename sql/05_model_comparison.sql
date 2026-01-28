---------------------------------------------------------------------
-- PHASE 5: MODEL COMPARISON & PROMOTION
---------------------------------------------------------------------
-- Compare CANDIDATE model vs PRODUCTION model
-- Promote only if CANDIDATE performs better

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE MLOPS_PROD_DB;
USE SCHEMA MODELS;

-- ============================================
-- 1. MODEL COMPARISON PROCEDURE
-- ============================================
CREATE OR REPLACE PROCEDURE MODELS.COMPARE_AND_PROMOTE()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    production_version VARCHAR;
    production_mae FLOAT;
    candidate_version VARCHAR;
    candidate_mae FLOAT;
    improvement_pct FLOAT;
    decision VARCHAR;
    decision_reason VARCHAR;
BEGIN
    -- Get current PRODUCTION model (if exists)
    SELECT MODEL_VERSION, MAE INTO :production_version, :production_mae
    FROM MODELS.MODEL_REGISTRY
    WHERE STATUS = 'PRODUCTION'
    ORDER BY TRAINING_DATE DESC
    LIMIT 1;
    
    -- Get latest CANDIDATE model
    SELECT MODEL_VERSION, MAE INTO :candidate_version, :candidate_mae
    FROM MODELS.MODEL_REGISTRY
    WHERE STATUS = 'CANDIDATE'
    ORDER BY TRAINING_DATE DESC
    LIMIT 1;
    
    -- If no candidate, nothing to compare
    IF (candidate_version IS NULL) THEN
        RETURN 'No CANDIDATE model found. Train a model first.';
    END IF;
    
    -- If no production model, promote candidate automatically
    IF (production_version IS NULL) THEN
        -- First model becomes production
        UPDATE MODELS.MODEL_REGISTRY
        SET STATUS = 'PRODUCTION', 
            PROMOTED_DATE = CURRENT_TIMESTAMP(),
            NOTES = 'First model - auto-promoted to PRODUCTION'
        WHERE MODEL_VERSION = :candidate_version;
        
        -- Log comparison
        INSERT INTO MODELS.MODEL_COMPARISON_LOG 
        (PRODUCTION_MODEL_VERSION, CANDIDATE_MODEL_VERSION, PRODUCTION_MAE, CANDIDATE_MAE, 
         IMPROVEMENT_PERCENT, DECISION, DECISION_REASON)
        VALUES (NULL, :candidate_version, NULL, :candidate_mae, 
                NULL, 'PROMOTE', 'First model - no production to compare');
        
        RETURN 'First model promoted to PRODUCTION: ' || :candidate_version;
    END IF;
    
    -- Calculate improvement (lower MAE is better)
    improvement_pct := ((production_mae - candidate_mae) / production_mae) * 100;
    
    -- Decision logic: Promote if MAE improved by at least 5%
    IF (improvement_pct >= 5) THEN
        decision := 'PROMOTE';
        decision_reason := 'MAE improved by ' || ROUND(improvement_pct, 2) || '%';
        
        -- Archive current production
        UPDATE MODELS.MODEL_REGISTRY
        SET STATUS = 'ARCHIVED',
            NOTES = 'Replaced by ' || :candidate_version
        WHERE MODEL_VERSION = :production_version;
        
        -- Promote candidate
        UPDATE MODELS.MODEL_REGISTRY
        SET STATUS = 'PRODUCTION',
            PROMOTED_DATE = CURRENT_TIMESTAMP(),
            NOTES = 'Promoted - ' || :decision_reason
        WHERE MODEL_VERSION = :candidate_version;
        
    ELSEIF (improvement_pct >= 0) THEN
        decision := 'KEEP_CURRENT';
        decision_reason := 'Improvement only ' || ROUND(improvement_pct, 2) || '% (threshold: 5%)';
        
        -- Archive candidate
        UPDATE MODELS.MODEL_REGISTRY
        SET STATUS = 'ARCHIVED',
            NOTES = 'Not promoted - ' || :decision_reason
        WHERE MODEL_VERSION = :candidate_version;
        
    ELSE
        decision := 'KEEP_CURRENT';
        decision_reason := 'Candidate is worse by ' || ROUND(ABS(improvement_pct), 2) || '%';
        
        -- Archive candidate
        UPDATE MODELS.MODEL_REGISTRY
        SET STATUS = 'ARCHIVED',
            NOTES = 'Not promoted - ' || :decision_reason
        WHERE MODEL_VERSION = :candidate_version;
    END IF;
    
    -- Log comparison result
    INSERT INTO MODELS.MODEL_COMPARISON_LOG 
    (PRODUCTION_MODEL_VERSION, CANDIDATE_MODEL_VERSION, PRODUCTION_MAE, CANDIDATE_MAE,
     IMPROVEMENT_PERCENT, DECISION, DECISION_REASON)
    VALUES (:production_version, :candidate_version, :production_mae, :candidate_mae,
            :improvement_pct, :decision, :decision_reason);
    
    RETURN 'Comparison complete. Decision: ' || :decision || ' - ' || :decision_reason;
END;
$$;

-- ============================================
-- 2. VIEW CURRENT MODEL STATUS
-- ============================================
-- Check current state before comparison
SELECT 
    MODEL_VERSION,
    MODEL_TYPE,
    STATUS,
    TRAINING_DATE,
    MAE,
    MAPE,
    NOTES
FROM MODELS.MODEL_REGISTRY
ORDER BY TRAINING_DATE DESC;

-- ============================================
-- 3. RUN COMPARISON
-- ============================================
CALL MODELS.COMPARE_AND_PROMOTE();

-- ============================================
-- 4. VERIFY RESULTS
-- ============================================
-- Check updated model status
SELECT 
    MODEL_VERSION,
    STATUS,
    MAE,
    PROMOTED_DATE,
    NOTES
FROM MODELS.MODEL_REGISTRY
ORDER BY TRAINING_DATE DESC;

-- Check comparison log
SELECT * FROM MODELS.MODEL_COMPARISON_LOG ORDER BY COMPARISON_DATE DESC LIMIT 5;

-- ============================================
-- 5. HELPER: GET PRODUCTION MODEL
-- ============================================
CREATE OR REPLACE PROCEDURE MODELS.GET_PRODUCTION_MODEL()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    prod_version VARCHAR;
    prod_mae FLOAT;
    prod_date TIMESTAMP_NTZ;
BEGIN
    SELECT MODEL_VERSION, MAE, TRAINING_DATE 
    INTO :prod_version, :prod_mae, :prod_date
    FROM MODELS.MODEL_REGISTRY
    WHERE STATUS = 'PRODUCTION'
    LIMIT 1;
    
    IF (prod_version IS NULL) THEN
        RETURN 'No PRODUCTION model found';
    END IF;
    
    RETURN 'PRODUCTION: ' || :prod_version || ' | MAE: ' || :prod_mae || ' | Trained: ' || :prod_date;
END;
$$;

-- Check production model
CALL MODELS.GET_PRODUCTION_MODEL();

-- ============================================
-- NEXT STEPS:
-- → Proceed to Phase 6: Monitoring
-- ============================================
