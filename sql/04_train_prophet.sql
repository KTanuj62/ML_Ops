---------------------------------------------------------------------
-- PHASE 4: PROPHET MODEL TRAINING
---------------------------------------------------------------------
-- Train Prophet model and register in Model Registry
-- Tracks metrics in MODEL_REGISTRY table

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE MLOPS_PROD_DB;

-- ============================================
-- 1. TRAINING PROCEDURE
-- ============================================
CREATE OR REPLACE PROCEDURE MODELS.TRAIN_PROPHET_MODEL()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'snowflake-ml-python', 'prophet', 'pandas', 'numpy')
HANDLER = 'train_model'
AS
$$
def train_model(session):
    import pandas as pd
    import numpy as np
    from datetime import datetime
    from prophet import Prophet
    from sklearn.metrics import mean_absolute_error, mean_squared_error
    from snowflake.ml.registry import Registry
    import warnings
    warnings.filterwarnings('ignore')
    
    log = []
    log.append(f"🚀 PROPHET TRAINING STARTED: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # ==========================================
    # STEP 1: Load features
    # ==========================================
    log.append("📊 Step 1: Loading features...")
    df = session.table("MLOPS_PROD_DB.FEATURES.FEATURE_STORE").to_pandas()
    log.append(f"   Total records: {len(df)}")
    
    if len(df) < 10:
        return "❌ Not enough data for training (need at least 10 records)"
    
    # ==========================================
    # STEP 2: Prepare data for Prophet
    # ==========================================
    log.append("🔧 Step 2: Preparing data...")
    
    # Prophet needs: ds (date), y (value)
    # We'll train a global model on aggregated data
    agg_df = df.groupby('DS').agg({'Y': 'sum'}).reset_index()
    agg_df.columns = ['ds', 'y']
    agg_df['ds'] = pd.to_datetime(agg_df['ds'])  # Ensure datetime type
    agg_df = agg_df.sort_values('ds')
    
    log.append(f"   Training periods: {len(agg_df)}")
    log.append(f"   Date range: {agg_df['ds'].min()} to {agg_df['ds'].max()}")
    
    # Train/Test split (80/20)
    split_idx = int(len(agg_df) * 0.8)
    train_df = agg_df.iloc[:split_idx].copy()
    test_df = agg_df.iloc[split_idx:].copy()
    
    log.append(f"   Train: {len(train_df)}, Test: {len(test_df)}")
    
    # ==========================================
    # STEP 3: Train Prophet model
    # ==========================================
    log.append("🤖 Step 3: Training Prophet model...")
    
    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=False,
        daily_seasonality=False,
        seasonality_mode='multiplicative'
    )
    model.fit(train_df)
    
    log.append("   ✓ Model trained successfully")
    
    # ==========================================
    # STEP 4: Evaluate model
    # ==========================================
    log.append("📈 Step 4: Evaluating model...")
    
    # Predict on test set
    future = model.make_future_dataframe(periods=len(test_df), freq='MS')
    forecast = model.predict(future)
    
    # Get predictions for test period
    test_predictions = forecast.tail(len(test_df))['yhat'].values
    test_actuals = test_df['y'].values
    
    # Calculate metrics
    mae = mean_absolute_error(test_actuals, test_predictions)
    rmse = np.sqrt(mean_squared_error(test_actuals, test_predictions))
    mape = np.mean(np.abs((test_actuals - test_predictions) / test_actuals)) * 100
    
    log.append(f"   MAE: {mae:,.0f}")
    log.append(f"   RMSE: {rmse:,.0f}")
    log.append(f"   MAPE: {mape:.2f}%")
    
    # ==========================================
    # STEP 5: Register model
    # ==========================================
    log.append("📦 Step 5: Registering model...")
    
    registry = Registry(session, database_name="MLOPS_PROD_DB", schema_name="MODELS")
    version = datetime.now().strftime("v%Y%m%d_%H%M%S")
    model_name = "SALES_PROPHET_MODEL"
    
    # Log model to registry
    # Convert dates to string for registry compatibility
    sample_df = train_df.head(10).copy()
    sample_df['ds'] = sample_df['ds'].astype(str)
    
    registry.log_model(
        model=model,
        model_name=model_name,
        version_name=version,
        sample_input_data=sample_df,
        metrics={"mae": float(mae), "rmse": float(rmse), "mape": float(mape)},
        comment=f"Prophet model. MAE:{mae:,.0f}, MAPE:{mape:.2f}%"
    )
    
    # Set as default
    reg_model = registry.get_model(model_name)
    reg_model.default = version
    
    log.append(f"   ✓ Registered: {model_name}/{version}")
    
    # ==========================================
    # STEP 6: Log to MODEL_REGISTRY table
    # ==========================================
    log.append("📝 Step 6: Logging to MODEL_REGISTRY...")
    
    session.sql(f"""
        INSERT INTO MLOPS_PROD_DB.MODELS.MODEL_REGISTRY 
        (MODEL_NAME, MODEL_VERSION, MODEL_TYPE, TRAINING_RECORDS, MAE, RMSE, MAPE, STATUS, NOTES)
        SELECT '{model_name}', '{version}', 'PROPHET', {len(train_df)}, 
               {mae}, {rmse}, {mape}, 'CANDIDATE', 
               'Newly trained model awaiting comparison'
    """).collect()
    
    log.append("✅ TRAINING COMPLETE!")
    log.append(f"   Model: {model_name}/{version}")
    log.append(f"   Status: CANDIDATE (pending comparison)")
    
    return "\n".join(log)
$$;

-- ============================================
-- 2. RUN TRAINING
-- ============================================
CALL MODELS.TRAIN_PROPHET_MODEL();

-- ============================================
-- 3. VERIFY MODEL REGISTRATION
-- ============================================
-- Check MODEL_REGISTRY table
SELECT * FROM MODELS.MODEL_REGISTRY ORDER BY TRAINING_DATE DESC LIMIT 5;

-- Check Snowflake Model Registry
SHOW MODELS IN SCHEMA MLOPS_PROD_DB.MODELS;

-- ============================================
-- NEXT STEPS:
-- → Proceed to Phase 5: Model Comparison
--   (Compare CANDIDATE vs PRODUCTION, promote if better)
-- ============================================
