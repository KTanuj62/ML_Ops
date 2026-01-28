# MLOps Production Pipeline

Production-grade MLOps pipeline for sales forecasting in Snowflake using Prophet.

## 🏗️ Architecture

```
CSV Data → @DATA_STAGE → RAW_SALES → FEATURE_STORE → PROPHET MODEL → PRODUCTION
                              ↓
                    Stream triggers Task
                              ↓
                    Auto-retrain pipeline
```

## 📁 Project Structure

```
ML_Ops/
├── .github/workflows/
│   └── deploy-snowflake.yml  # CI/CD pipeline
├── sql/
│   ├── 01_setup.sql          # Database setup (run once)
│   ├── 02_data_validation.sql
│   ├── 03_feature_store.sql
│   ├── 04_train_prophet.sql
│   ├── 05_model_comparison.sql
│   ├── 06_monitoring.sql
│   ├── 07_automation.sql
│   └── 08_streamlit_dashboard.sql
├── streamlit/
│   └── dashboard.py          # Monitoring dashboard
├── data/
│   └── sales_batch_*.csv
└── README.md
```

## 🚀 CI/CD Pipeline

Push to `main` → GitHub Actions → Auto-deploy to Snowflake

### Required Secrets (GitHub → Settings → Secrets)

| Secret | Value |
|--------|-------|
| `SNOWFLAKE_ACCOUNT` | `YQTKXAW-FTB76062` |
| `SNOWFLAKE_USER` | `SUMITOMO` |
| `SNOWFLAKE_PASSWORD` | Your password |

## 📊 Schemas

| Schema | Purpose |
|--------|---------|
| `RAW` | Raw ingested data |
| `STAGING` | Validation logs |
| `FEATURES` | Feature store |
| `MODELS` | Model registry |
| `MONITORING` | Drift & pipeline logs |

## 🔄 Tasks (Suspended for cost savings)

```sql
-- Resume when needed
ALTER TASK RAW.TRAINING_PIPELINE_TASK RESUME;
ALTER TASK RAW.DATA_LOAD_TASK RESUME;
```
