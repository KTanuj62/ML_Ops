---------------------------------------------------------------------
-- PHASE 8: STREAMLIT MONITORING DASHBOARD
---------------------------------------------------------------------
-- The dashboard is created manually in Snowflake Streamlit UI
-- This file documents what was deployed

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE MLOPS_PROD_DB;

-- ============================================
-- DASHBOARD DEPLOYED
-- ============================================
-- Name: MLOPS_DASHBOARD
-- Database: MLOPS_PROD_DB
-- Schema: MONITORING
-- Warehouse: COMPUTE_WH
-- 
-- The Python code is in: streamlit/dashboard.py
-- 
-- To redeploy: Copy streamlit/dashboard.py content
-- into Snowflake Streamlit UI
-- ============================================
