# Snowflake Native dbt - Data Vault 2.0 Project

Enterprise Data Vault 2.0 implementation using **Snowflake Native dbt** with Dynamic Tables.

## Quick Start

```sql
-- Run dbt models
EXECUTE DBT PROJECT DEMO_DB.DBT_DEV.DATAVAULT_CUSTOMERS ARGS='run';

-- Run tests
EXECUTE DBT PROJECT DEMO_DB.DBT_DEV.DATAVAULT_CUSTOMERS ARGS='test';
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         GitHub                                   │
│   github.com/PeckaMilan/snowflake-dbt-datavault                 │
└─────────────────────┬───────────────────────────────────────────┘
                      │ Git Integration
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Snowflake                                     │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  GIT_REPOS.DBT_PROJECTS.DATAVAULT_DBT_REPO                │  │
│  │  (Git Repository Object)                                   │  │
│  └─────────────────────┬─────────────────────────────────────┘  │
│                        │                                         │
│                        ▼                                         │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  DEMO_DB.DBT_DEV.DATAVAULT_CUSTOMERS                      │  │
│  │  (dbt Project Object)                                      │  │
│  │  dbt version: 1.9.4                                        │  │
│  └─────────────────────┬─────────────────────────────────────┘  │
│                        │ EXECUTE DBT PROJECT                     │
│                        ▼                                         │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  DEMO_DB.DBT_DEV_RAW_VAULT (Dynamic Tables)               │  │
│  │  ┌─────────────────┐    ┌─────────────────────────────┐   │  │
│  │  │  HUB_CUSTOMERS  │◄───│  SAT_CUSTOMERS_DETAILS      │   │  │
│  │  │  (Business Key) │    │  (Payload + HASHDIFF)       │   │  │
│  │  │  - CUSTOMER_ID  │    │  - FIRST_NAME, LAST_NAME    │   │  │
│  │  │  - HK_H_CUSTOMERS│   │  - CUSTOMER_EMAIL           │   │  │
│  │  └─────────────────┘    │  - CITY, COUNTRY            │   │  │
│  │                         │  - CREATED_DATE             │   │  │
│  │                         └─────────────────────────────┘   │  │
│  └───────────────────────────────────────────────────────────┘  │
│                        ▲                                         │
│                        │ Source                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  DEMO_DB.DBT_DEV.RAW_CUSTOMERS                            │  │
│  │  (Source Table)                                            │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
snowflake_dbt_project/
├── dbt_project.yml           # dbt configuration
├── profiles.yml              # Snowflake connection profile
├── macros/
│   └── dv_hash.sql           # Data Vault hash macros (MD5)
├── models/
│   ├── staging/
│   │   └── _sources.yml      # Source definitions
│   └── raw_vault/
│       ├── _raw_vault.yml    # Model documentation & tests
│       ├── hubs/
│       │   └── hub_customers.sql
│       └── satellites/
│           └── sat_customers_details.sql
└── *.sql                     # Setup & utility scripts
```

## Data Vault 2.0 Patterns

### Hub Pattern
```sql
SELECT
    HK_H_CUSTOMERS,           -- Hash Key (MD5)
    CUSTOMER_ID,              -- Business Key
    EFFECTIVE_TS,             -- Business timestamp
    DWH_VALID_TS,             -- Load timestamp
    DSS_RECORD_SOURCE         -- Source system
FROM (...)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY HK_H_CUSTOMERS
    ORDER BY DWH_VALID_TS
) = 1                         -- Deduplication
```

### Satellite Pattern
```sql
SELECT
    HK_H_CUSTOMERS,           -- Foreign Key to Hub
    HASHDIFF,                 -- Change detection hash
    FIRST_NAME, LAST_NAME,    -- Payload columns
    EFFECTIVE_TS,
    DWH_VALID_TS,
    DSS_RECORD_SOURCE
FROM (...)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY HK_H_CUSTOMERS, HASHDIFF
    ORDER BY DWH_VALID_TS
) = 1                         -- Deduplication by hash + hashdiff
```

## SQL Commands Reference

### Setup (One-time)

```sql
-- 1. Create API Integration for GitHub
CREATE OR REPLACE API INTEGRATION git_api_integration_public
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/PeckaMilan/')
    ENABLED = TRUE;

-- 2. Create Git Repository Object
CREATE OR REPLACE GIT REPOSITORY GIT_REPOS.DBT_PROJECTS.DATAVAULT_DBT_REPO
    API_INTEGRATION = git_api_integration_public
    ORIGIN = 'https://github.com/PeckaMilan/snowflake-dbt-datavault.git';

-- 3. Create dbt Project
CREATE OR REPLACE DBT PROJECT DEMO_DB.DBT_DEV.DATAVAULT_CUSTOMERS
    FROM '@GIT_REPOS.DBT_PROJECTS.DATAVAULT_DBT_REPO/branches/main';
```

### Daily Operations

```sql
-- Fetch latest from Git
ALTER GIT REPOSITORY GIT_REPOS.DBT_PROJECTS.DATAVAULT_DBT_REPO FETCH;

-- Run all models
EXECUTE DBT PROJECT DEMO_DB.DBT_DEV.DATAVAULT_CUSTOMERS ARGS='run';

-- Run tests
EXECUTE DBT PROJECT DEMO_DB.DBT_DEV.DATAVAULT_CUSTOMERS ARGS='test';

-- Run specific model
EXECUTE DBT PROJECT DEMO_DB.DBT_DEV.DATAVAULT_CUSTOMERS ARGS='run --select hub_customers';

-- Full refresh (rebuild Dynamic Tables)
EXECUTE DBT PROJECT DEMO_DB.DBT_DEV.DATAVAULT_CUSTOMERS ARGS='run --full-refresh';
```

### Monitoring

```sql
-- Show dbt projects
SHOW DBT PROJECTS IN DATABASE DEMO_DB;

-- Describe project
DESCRIBE DBT PROJECT DEMO_DB.DBT_DEV.DATAVAULT_CUSTOMERS;

-- Show Dynamic Tables
SHOW DYNAMIC TABLES IN SCHEMA DEMO_DB.DBT_DEV_RAW_VAULT;

-- Check data
SELECT * FROM DEMO_DB.DBT_DEV_RAW_VAULT.HUB_CUSTOMERS;
SELECT * FROM DEMO_DB.DBT_DEV_RAW_VAULT.SAT_CUSTOMERS_DETAILS;
```

### Scheduling

```sql
-- Create scheduled task for daily dbt run
CREATE OR REPLACE TASK DEMO_DB.DBT_DEV.DAILY_DBT_RUN
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 6 * * * UTC'  -- Every day at 6 AM UTC
AS
EXECUTE DBT PROJECT DEMO_DB.DBT_DEV.DATAVAULT_CUSTOMERS ARGS='run';

-- Enable the task
ALTER TASK DEMO_DB.DBT_DEV.DAILY_DBT_RUN RESUME;

-- Check task status
SHOW TASKS IN SCHEMA DEMO_DB.DBT_DEV;
```

## Hash Macros

### dv_hash (for Hub/Link hash keys)
```sql
{{ dv_hash(['CUSTOMER_ID'], 'HK_H_CUSTOMERS') }}
-- Generates: MD5(COALESCE(CAST(CUSTOMER_ID AS VARCHAR), '')) AS HK_H_CUSTOMERS
```

### dv_hashdiff (for Satellite change detection)
```sql
{{ dv_hashdiff(['FIRST_NAME', 'LAST_NAME', 'EMAIL'], 'HASHDIFF') }}
-- Generates: MD5(COALESCE(...) || '|' || COALESCE(...) || '|' || ...) AS HASHDIFF
```

## Tests

| Test | Model | Status |
|------|-------|--------|
| unique_hub_customers_HK_H_CUSTOMERS | hub_customers | PASS |
| not_null_hub_customers_HK_H_CUSTOMERS | hub_customers | PASS |
| not_null_hub_customers_CUSTOMER_ID | hub_customers | PASS |
| not_null_sat_customers_details_HK_H_CUSTOMERS | sat_customers_details | PASS |
| not_null_sat_customers_details_HASHDIFF | sat_customers_details | PASS |

## Adding New Models

1. **Create model file** in appropriate directory:
   - Hubs: `models/raw_vault/hubs/hub_<entity>.sql`
   - Satellites: `models/raw_vault/satellites/sat_<entity>_<descriptor>.sql`

2. **Add documentation** in `models/raw_vault/_raw_vault.yml`

3. **Commit and push** to GitHub:
   ```bash
   git add -A
   git commit -m "Add new model"
   git push
   ```

4. **Fetch and run** in Snowflake:
   ```sql
   ALTER GIT REPOSITORY GIT_REPOS.DBT_PROJECTS.DATAVAULT_DBT_REPO FETCH;
   EXECUTE DBT PROJECT DEMO_DB.DBT_DEV.DATAVAULT_CUSTOMERS ARGS='run';
   ```

## Troubleshooting

### "Profile not found" error
Ensure `profiles.yml` exists in the repository root with profile name matching `dbt_project.yml`.

### Dynamic Table not refreshing
```sql
-- Check Dynamic Table status
SHOW DYNAMIC TABLES LIKE 'HUB_CUSTOMERS' IN SCHEMA DBT_DEV_RAW_VAULT;

-- Force refresh
EXECUTE DBT PROJECT ... ARGS='run --full-refresh --select hub_customers';
```

### Git changes not reflected
```sql
-- Fetch latest from Git
ALTER GIT REPOSITORY GIT_REPOS.DBT_PROJECTS.DATAVAULT_DBT_REPO FETCH;

-- Verify files
LIST @GIT_REPOS.DBT_PROJECTS.DATAVAULT_DBT_REPO/branches/main/;
```

## Resources

- [Snowflake Native dbt Documentation](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake)
- [EXECUTE DBT PROJECT Reference](https://docs.snowflake.com/en/sql-reference/sql/execute-dbt-project)
- [Data Vault 2.0 Methodology](https://www.data-vault.co.uk/)
