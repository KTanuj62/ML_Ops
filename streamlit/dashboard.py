import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

# Get session
session = get_active_session()

# Page config
st.set_page_config(page_title="MLOps Dashboard", layout="wide")
st.title("🚀 MLOps Production Dashboard")
st.markdown("---")

# ============================================
# KPI SECTION
# ============================================
col1, col2, col3, col4 = st.columns(4)

# Production Model
prod_model = session.sql("""
    SELECT MODEL_VERSION, MAE, TRAINING_DATE 
    FROM MLOPS_PROD_DB.MODELS.MODEL_REGISTRY 
    WHERE STATUS = 'PRODUCTION' LIMIT 1
""").collect()

if prod_model:
    col1.metric("🏆 Production Model", prod_model[0]['MODEL_VERSION'])
    col2.metric("📊 MAE", f"{prod_model[0]['MAE']:,.0f}")
else:
    col1.metric("🏆 Production Model", "None")
    col2.metric("📊 MAE", "N/A")

# Data counts
raw_count = session.sql("SELECT COUNT(*) AS CNT FROM MLOPS_PROD_DB.RAW.RAW_SALES").collect()[0]['CNT']
feature_count = session.sql("SELECT COUNT(*) AS CNT FROM MLOPS_PROD_DB.FEATURES.FEATURE_STORE").collect()[0]['CNT']

col3.metric("📁 Raw Records", f"{raw_count:,}")
col4.metric("🔧 Features", f"{feature_count:,}")

st.markdown("---")

# ============================================
# MODEL HISTORY
# ============================================
st.subheader("📈 Model Training History")

model_df = session.sql("""
    SELECT MODEL_VERSION, MODEL_TYPE, STATUS, TRAINING_DATE, MAE, RMSE, MAPE, NOTES
    FROM MLOPS_PROD_DB.MODELS.MODEL_REGISTRY
    ORDER BY TRAINING_DATE DESC
    LIMIT 10
""").to_pandas()

if len(model_df) > 0:
    st.dataframe(model_df, use_container_width=True)
else:
    st.info("No models trained yet")

# ============================================
# DRIFT MONITORING
# ============================================
st.markdown("---")
st.subheader("🔍 Data Drift Monitoring")

col1, col2 = st.columns(2)

with col1:
    drift_df = session.sql("""
        SELECT CHECK_DATE, METRIC_NAME, BASELINE_VALUE, CURRENT_VALUE, DRIFT_SCORE, DRIFT_STATUS
        FROM MLOPS_PROD_DB.MONITORING.DRIFT_METRICS
        ORDER BY CHECK_DATE DESC
        LIMIT 10
    """).to_pandas()
    
    if len(drift_df) > 0:
        st.dataframe(drift_df, use_container_width=True)
    else:
        st.info("No drift data yet")

with col2:
    latest_drift = session.sql("""
        SELECT DRIFT_STATUS FROM MLOPS_PROD_DB.MONITORING.DRIFT_METRICS 
        WHERE METRIC_NAME = 'Y_MEAN'
        ORDER BY CHECK_DATE DESC LIMIT 1
    """).collect()
    
    if latest_drift:
        status = latest_drift[0]['DRIFT_STATUS']
        if status == 'NORMAL' or status == 'BASELINE':
            st.success(f"✅ Drift Status: {status}")
        elif status == 'WARNING':
            st.warning(f"⚠️ Drift Status: {status}")
        else:
            st.error(f"🚨 Drift Status: {status}")
    else:
        st.info("No drift data")

# ============================================
# COMPARISON LOG
# ============================================
st.markdown("---")
st.subheader("⚖️ Model Comparison History")

comparison_df = session.sql("""
    SELECT COMPARISON_DATE, PRODUCTION_MODEL_VERSION, CANDIDATE_MODEL_VERSION,
           PRODUCTION_MAE, CANDIDATE_MAE, IMPROVEMENT_PERCENT, DECISION, DECISION_REASON
    FROM MLOPS_PROD_DB.MODELS.MODEL_COMPARISON_LOG
    ORDER BY COMPARISON_DATE DESC
    LIMIT 10
""").to_pandas()

if len(comparison_df) > 0:
    st.dataframe(comparison_df, use_container_width=True)
else:
    st.info("No comparisons logged yet")

# ============================================
# PIPELINE RUNS
# ============================================
st.markdown("---")
st.subheader("🔄 Pipeline Run History")

runs_df = session.sql("""
    SELECT RUN_TIME, PHASE, STATUS, RECORDS_PROCESSED
    FROM MLOPS_PROD_DB.MONITORING.PIPELINE_RUNS
    ORDER BY RUN_TIME DESC
    LIMIT 10
""").to_pandas()

if len(runs_df) > 0:
    st.dataframe(runs_df, use_container_width=True)
else:
    st.info("No pipeline runs yet")

# ============================================
# STREAM DATA LOG
# ============================================
st.markdown("---")
st.subheader("📥 Data Load Triggers")

log_df = session.sql("""
    SELECT RUN_TIME, NEW_ROWS, TRIGGER_TYPE
    FROM MLOPS_PROD_DB.MONITORING.PIPELINE_RUN_LOG
    ORDER BY RUN_TIME DESC
    LIMIT 10
""").to_pandas()

if len(log_df) > 0:
    st.dataframe(log_df, use_container_width=True)
else:
    st.info("No data load triggers yet")

st.markdown("---")
st.caption("MLOps Production Pipeline Dashboard | Auto-refreshes with new data")
