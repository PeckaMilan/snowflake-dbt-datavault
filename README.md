# Snowflake Native dbt - Data Vault 2.0 Project

Enterprise Data Vault 2.0 implementation using **Snowflake Native dbt** with Dynamic Tables.

## Quick Start

```sql
-- Fetch latest from GitHub
ALTER GIT REPOSITORY DEMO_DB.GIT_REPOS.DATAVAULT_REPO FETCH;

-- Run dbt models
EXECUTE DBT PROJECT DEMO_DB.DBT_DEV.DATAVAULT_MULTI ARGS='run';

-- Run tests
EXECUTE DBT PROJECT DEMO_DB.DBT_DEV.DATAVAULT_MULTI ARGS='test';
```

## Current Deployment

**9 Models deployed successfully:**

| Type | Model | Source Table |
|------|-------|--------------|
| Hub | HUB_CUSTOMER | RAW_CUSTOMERS |
| Hub | HUB_ORDER | RAW_ORDERS |
| Hub | HUB_PRODUCT | RAW_PRODUCTS |
| Satellite | SAT_CUSTOMER_DETAILS | RAW_CUSTOMERS |
| Satellite | SAT_ORDER_DETAILS | RAW_ORDERS |
| Link | LINK_ORDER_CUSTOMER | RAW_ORDERS |
| Link | LINK_ORDER_PRODUCT | RAW_ORDERS |

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
│  │  DEMO_DB.GIT_REPOS.DATAVAULT_REPO                         │  │
│  │  (Git Repository Object)                                   │  │
│  └─────────────────────┬─────────────────────────────────────┘  │
│                        │                                         │
│                        ▼                                         │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  DEMO_DB.DBT_DEV.DATAVAULT_MULTI                          │  │
│  │  (dbt Project Object)                                      │  │
│  └─────────────────────┬─────────────────────────────────────┘  │
│                        │ EXECUTE DBT PROJECT                     │
│                        ▼                                         │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  DEMO_DB.DBT_DEV_RAW_VAULT (Dynamic Tables)               │  │
│  │                                                            │  │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐          │  │
│  │  │HUB_CUSTOMER │ │ HUB_ORDER   │ │ HUB_PRODUCT │          │  │
│  │  └──────┬──────┘ └──────┬──────┘ └──────┬──────┘          │  │
│  │         │               │               │                  │  │
│  │         ▼               ▼               ▼                  │  │
│  │  ┌──────────────┐ ┌───────────────────────────┐           │  │
│  │  │SAT_CUSTOMER_ │ │   LINK_ORDER_CUSTOMER     │           │  │
│  │  │DETAILS       │ │   LINK_ORDER_PRODUCT      │           │  │
│  │  └──────────────┘ └───────────────────────────┘           │  │
│  │                                                            │  │
│  │  ┌──────────────────────────────────────────┐             │  │
│  │  │  SAT_ORDER_DETAILS                        │             │  │
│  │  └──────────────────────────────────────────┘             │  │
│  └───────────────────────────────────────────────────────────┘  │
│                        ▲                                         │
│                        │ Source                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  DEMO_DB.DBT_DEV.RAW_CUSTOMERS                            │  │
│  │  DEMO_DB.DBT_DEV.RAW_ORDERS                               │  │
│  │  DEMO_DB.DBT_DEV.RAW_PRODUCTS                             │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
snowflake_dbt_project/
├── dbt_project.yml           # dbt configuration
├── profiles.yml              # Snowflake connection profile
├── macros/
│   └── dv_hash.sql           # Data Vault hash macro (MD5)
├── models/
│   ├── staging/
│   │   └── _sources.yml      # Source definitions
│   └── raw_vault/
│       ├── _raw_vault.yml    # Model documentation & tests
│       ├── hubs/
│       │   ├── HUB_CUSTOMER.sql
│       │   ├── HUB_ORDER.sql
│       │   └── HUB_PRODUCT.sql
│       ├── satellites/
│       │   ├── SAT_CUSTOMER_DETAILS.sql
│       │   └── SAT_ORDER_DETAILS.sql
│       └── links/
│           ├── LINK_ORDER_CUSTOMER.sql
│           └── LINK_ORDER_PRODUCT.sql
└── *.sql                     # Setup & utility scripts
```

## Data Vault 2.0 Patterns

### Technical Columns

All models include these auto-generated columns:

| Column | Value | Purpose |
|--------|-------|---------|
| `LOAD_TS` | `CURRENT_TIMESTAMP()` | Load timestamp |
| `RECORD_SOURCE` | `'TABLE_NAME'` | Source system identifier |
| `HASHDIFF` | `{{ dv_hash([...]) }}` | Change detection (satellites only) |

### Hub Pattern
```sql
SELECT
    HK_H_HUB_CUSTOMER,        -- Hash Key (MD5)
    CUSTOMER_ID,              -- Business Key
    LOAD_TS,                  -- Load timestamp
    RECORD_SOURCE             -- Source system
FROM (
    SELECT
        {{ dv_hash(['CUSTOMER_ID'], 'HK_H_HUB_CUSTOMER') }},
        CUSTOMER_ID,
        CURRENT_TIMESTAMP() AS LOAD_TS,
        'RAW_CUSTOMERS' AS RECORD_SOURCE
    FROM {{ source('demo_db_dbt_dev', 'RAW_CUSTOMERS') }}
    WHERE CUSTOMER_ID IS NOT NULL
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY HK_H_HUB_CUSTOMER
    ORDER BY LOAD_TS
) = 1
```

### Satellite Pattern
```sql
SELECT
    HK_H_HUB_CUSTOMER,        -- Foreign Key to Hub
    HASHDIFF,                 -- Change detection hash
    FIRST_NAME, LAST_NAME,    -- Payload columns
    LOAD_TS,
    RECORD_SOURCE
FROM (
    SELECT
        {{ dv_hash(['CUSTOMER_ID'], 'HK_H_HUB_CUSTOMER') }},
        {{ dv_hash(['FIRST_NAME', 'LAST_NAME', ...], 'HASHDIFF') }},
        FIRST_NAME, LAST_NAME, ...
        CURRENT_TIMESTAMP() AS LOAD_TS,
        'RAW_CUSTOMERS' AS RECORD_SOURCE
    FROM {{ source('demo_db_dbt_dev', 'RAW_CUSTOMERS') }}
    WHERE CUSTOMER_ID IS NOT NULL
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY HK_H_HUB_CUSTOMER, HASHDIFF
    ORDER BY LOAD_TS
) = 1
```

### Link Pattern
```sql
SELECT
    HK_L_LINK_ORDER_CUSTOMER, -- Link Hash Key
    HK_H_ORDER,               -- Hub 1 Hash Key
    HK_H_CUSTOMER,            -- Hub 2 Hash Key
    ORDER_ID,                 -- Business Key 1
    CUSTOMER_ID,              -- Business Key 2
    LOAD_TS,
    RECORD_SOURCE
FROM (
    SELECT
        {{ dv_hash(['ORDER_ID', 'CUSTOMER_ID'], 'HK_L_LINK_ORDER_CUSTOMER') }},
        {{ dv_hash(['ORDER_ID'], 'HK_H_ORDER') }},
        {{ dv_hash(['CUSTOMER_ID'], 'HK_H_CUSTOMER') }},
        ORDER_ID,
        CUSTOMER_ID,
        CURRENT_TIMESTAMP() AS LOAD_TS,
        'RAW_ORDERS' AS RECORD_SOURCE
    FROM {{ source('demo_db_dbt_dev', 'RAW_ORDERS') }}
    WHERE ORDER_ID IS NOT NULL AND CUSTOMER_ID IS NOT NULL
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY HK_L_LINK_ORDER_CUSTOMER
    ORDER BY LOAD_TS
) = 1
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
CREATE OR REPLACE GIT REPOSITORY DEMO_DB.GIT_REPOS.DATAVAULT_REPO
    API_INTEGRATION = git_api_integration_public
    ORIGIN = 'https://github.com/PeckaMilan/snowflake-dbt-datavault.git';

-- 3. Create dbt Project
CREATE OR REPLACE DBT PROJECT DEMO_DB.DBT_DEV.DATAVAULT_MULTI
    FROM '@DEMO_DB.GIT_REPOS.DATAVAULT_REPO/branches/main';
```

### Daily Operations

```sql
-- Fetch latest from Git
ALTER GIT REPOSITORY DEMO_DB.GIT_REPOS.DATAVAULT_REPO FETCH;

-- Run all models
EXECUTE DBT PROJECT DEMO_DB.DBT_DEV.DATAVAULT_MULTI ARGS='run';

-- Run tests
EXECUTE DBT PROJECT DEMO_DB.DBT_DEV.DATAVAULT_MULTI ARGS='test';

-- Run specific model
EXECUTE DBT PROJECT DEMO_DB.DBT_DEV.DATAVAULT_MULTI ARGS='run --select HUB_CUSTOMER';

-- Full refresh (rebuild Dynamic Tables)
EXECUTE DBT PROJECT DEMO_DB.DBT_DEV.DATAVAULT_MULTI ARGS='run --full-refresh';
```

### Monitoring

```sql
-- Show dbt projects
SHOW DBT PROJECTS IN DATABASE DEMO_DB;

-- Describe project
DESCRIBE DBT PROJECT DEMO_DB.DBT_DEV.DATAVAULT_MULTI;

-- Show Dynamic Tables
SHOW DYNAMIC TABLES IN SCHEMA DEMO_DB.DBT_DEV_RAW_VAULT;

-- Check data
SELECT * FROM DEMO_DB.DBT_DEV_RAW_VAULT.HUB_CUSTOMER LIMIT 10;
SELECT * FROM DEMO_DB.DBT_DEV_RAW_VAULT.SAT_CUSTOMER_DETAILS LIMIT 10;
SELECT * FROM DEMO_DB.DBT_DEV_RAW_VAULT.LINK_ORDER_CUSTOMER LIMIT 10;
```

### Scheduling

```sql
-- Create scheduled task for automatic deployment
CREATE OR REPLACE TASK DBT_AUTO_DEPLOY
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 */6 * * * UTC'
AS
BEGIN
    ALTER GIT REPOSITORY DEMO_DB.GIT_REPOS.DATAVAULT_REPO FETCH;
    EXECUTE DBT PROJECT DEMO_DB.DBT_DEV.DATAVAULT_MULTI ARGS='run';
END;

ALTER TASK DBT_AUTO_DEPLOY RESUME;
```

## Hash Macro

### dv_hash (for all hash keys)
```sql
{{ dv_hash(['CUSTOMER_ID'], 'HK_H_CUSTOMER') }}
-- Generates: MD5(COALESCE(CAST(CUSTOMER_ID AS VARCHAR), '')) AS HK_H_CUSTOMER

{{ dv_hash(['FIRST_NAME', 'LAST_NAME', 'EMAIL'], 'HASHDIFF') }}
-- Generates: MD5(COALESCE(...) || '|' || COALESCE(...) || '|' || ...) AS HASHDIFF
```

## Tests

| Test | Model | Status |
|------|-------|--------|
| not_null_HUB_CUSTOMER_CUSTOMER_ID | HUB_CUSTOMER | PASS |
| not_null_HUB_ORDER_ORDER_ID | HUB_ORDER | PASS |
| not_null_HUB_PRODUCT_PRODUCT_ID | HUB_PRODUCT | PASS |
| not_null_LINK_ORDER_CUSTOMER_ORDER_ID | LINK_ORDER_CUSTOMER | PASS |
| not_null_LINK_ORDER_CUSTOMER_CUSTOMER_ID | LINK_ORDER_CUSTOMER | PASS |
| not_null_LINK_ORDER_PRODUCT_ORDER_ID | LINK_ORDER_PRODUCT | PASS |
| not_null_LINK_ORDER_PRODUCT_PRODUCT_ID | LINK_ORDER_PRODUCT | PASS |

## Adding New Models

1. **Generate with VaultFlow App** - Use the Multi-Table Scanner in Snowflake Native App

2. **Or manually create** model file in appropriate directory:
   - Hubs: `models/raw_vault/hubs/HUB_<ENTITY>.sql`
   - Satellites: `models/raw_vault/satellites/SAT_<ENTITY>_<DESCRIPTOR>.sql`
   - Links: `models/raw_vault/links/LINK_<ENTITY1>_<ENTITY2>.sql`

3. **Add documentation** in `models/raw_vault/_raw_vault.yml`

4. **Commit and push** to GitHub:
   ```bash
   git add -A
   git commit -m "Add new model"
   git push
   ```

5. **Fetch and run** in Snowflake:
   ```sql
   ALTER GIT REPOSITORY DEMO_DB.GIT_REPOS.DATAVAULT_REPO FETCH;
   EXECUTE DBT PROJECT DEMO_DB.DBT_DEV.DATAVAULT_MULTI ARGS='run';
   ```

## Troubleshooting

### "Profile not found" error
Ensure `profiles.yml` exists in the repository root with profile name matching `dbt_project.yml`.

### Dynamic Table not refreshing
```sql
-- Check Dynamic Table status
SHOW DYNAMIC TABLES LIKE 'HUB_CUSTOMER' IN SCHEMA DBT_DEV_RAW_VAULT;

-- Force refresh
EXECUTE DBT PROJECT DEMO_DB.DBT_DEV.DATAVAULT_MULTI ARGS='run --full-refresh --select HUB_CUSTOMER';
```

### Git changes not reflected
```sql
-- Fetch latest from Git
ALTER GIT REPOSITORY DEMO_DB.GIT_REPOS.DATAVAULT_REPO FETCH;

-- Verify files
LIST @DEMO_DB.GIT_REPOS.DATAVAULT_REPO/branches/main/;
```

### Invalid identifier errors
The generator creates technical columns (`LOAD_TS`, `RECORD_SOURCE`) as computed values - they should NOT exist in source tables.

## Resources

- [Snowflake Native dbt Documentation](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake)
- [EXECUTE DBT PROJECT Reference](https://docs.snowflake.com/en/sql-reference/sql/execute-dbt-project)
- [Data Vault 2.0 Methodology](https://www.data-vault.co.uk/)
- [VaultFlow Neuro-Symbolic Engine](../neuro_symbolic_engine/docs/ARCHITECTURE.md)
