-- ============================================================================
-- Snowflake Native dbt Setup
-- ============================================================================
-- This script sets up dbt to run natively in Snowflake
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- 1. Create database and schema for dbt project files
CREATE DATABASE IF NOT EXISTS DBT_PROJECTS;
CREATE SCHEMA IF NOT EXISTS DBT_PROJECTS.DATA_VAULT_CUSTOMERS;

USE DATABASE DBT_PROJECTS;
USE SCHEMA DATA_VAULT_CUSTOMERS;

-- 2. Create internal stage for dbt project files
CREATE OR REPLACE STAGE DBT_PROJECT_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for dbt project files';

-- 3. After uploading files to stage, create Git repository object
-- NOTE: For production, use actual Git repo. For testing, we use stage.

-- ============================================================================
-- OPTION A: Using Git Repository (Recommended for Production)
-- ============================================================================
-- First, create a secret for Git authentication (if private repo):
-- CREATE OR REPLACE SECRET git_secret
--     TYPE = password
--     USERNAME = 'your-github-username'
--     PASSWORD = 'your-github-token';

-- Create API integration for Git:
-- CREATE OR REPLACE API INTEGRATION git_api_integration
--     API_PROVIDER = git_https_api
--     API_ALLOWED_PREFIXES = ('https://github.com/your-org/')
--     ENABLED = TRUE;

-- Create Git repository:
-- CREATE OR REPLACE GIT REPOSITORY data_vault_repo
--     API_INTEGRATION = git_api_integration
--     GIT_CREDENTIALS = git_secret
--     ORIGIN = 'https://github.com/your-org/your-dbt-project.git';

-- ============================================================================
-- OPTION B: Using Internal Stage (Quick Testing)
-- ============================================================================

-- After uploading files via Snowsight or PUT command, list them:
LIST @DBT_PROJECT_STAGE;

-- ============================================================================
-- 4. Create dbt Connection (for Native dbt)
-- ============================================================================
-- This is configured in Snowsight UI under:
-- Data > Databases > (your db) > dbt Projects > + dbt Project

-- Connection settings:
-- - Warehouse: COMPUTE_WH
-- - Database: DEMO_DB
-- - Schema: DBT_DEV (for staging views)
-- - Target Schema: DBT_DEV_RAW_VAULT (for raw vault tables)

-- ============================================================================
-- 5. Verify source data exists
-- ============================================================================
SELECT 'RAW_CUSTOMERS row count' AS check_name, COUNT(*) AS result
FROM DEMO_DB.DBT_DEV.RAW_CUSTOMERS;

-- Show table structure
DESCRIBE TABLE DEMO_DB.DBT_DEV.RAW_CUSTOMERS;

-- ============================================================================
-- ALTERNATIVE: Direct Dynamic Table Creation (No dbt required)
-- ============================================================================
-- If dbt setup is complex, create Dynamic Tables directly:

USE DATABASE DEMO_DB;
CREATE SCHEMA IF NOT EXISTS DBT_DEV_RAW_VAULT;
USE SCHEMA DBT_DEV_RAW_VAULT;

-- Hub: Customers
CREATE OR REPLACE DYNAMIC TABLE HUB_CUSTOMERS
    TARGET_LAG = '1 day'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    HK_H_CUSTOMERS,
    CUSTOMER_ID,
    EFFECTIVE_TS,
    DWH_VALID_TS,
    DSS_RECORD_SOURCE
FROM (
    SELECT
        MD5(COALESCE(CAST(CUSTOMER_ID AS VARCHAR), '')) AS HK_H_CUSTOMERS,
        CUSTOMER_ID,
        CURRENT_TIMESTAMP() AS EFFECTIVE_TS,
        CURRENT_TIMESTAMP() AS DWH_VALID_TS,
        'DEMO_DB.DBT_DEV.RAW_CUSTOMERS' AS DSS_RECORD_SOURCE
    FROM DEMO_DB.DBT_DEV.RAW_CUSTOMERS
    WHERE CUSTOMER_ID IS NOT NULL
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY HK_H_CUSTOMERS
    ORDER BY DWH_VALID_TS
) = 1;

-- Satellite: Customer Details
CREATE OR REPLACE DYNAMIC TABLE SAT_CUSTOMERS_DETAILS
    TARGET_LAG = '1 day'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    HK_H_CUSTOMERS,
    HASHDIFF,
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    CITY,
    COUNTRY,
    CREATED_DATE,
    EFFECTIVE_TS,
    DWH_VALID_TS,
    DSS_RECORD_SOURCE
FROM (
    SELECT
        MD5(COALESCE(CAST(CUSTOMER_ID AS VARCHAR), '')) AS HK_H_CUSTOMERS,
        MD5(
            COALESCE(CAST(FIRST_NAME AS VARCHAR), '') || '|' ||
            COALESCE(CAST(LAST_NAME AS VARCHAR), '') || '|' ||
            COALESCE(CAST(EMAIL AS VARCHAR), '') || '|' ||
            COALESCE(CAST(CITY AS VARCHAR), '') || '|' ||
            COALESCE(CAST(COUNTRY AS VARCHAR), '') || '|' ||
            COALESCE(CAST(CREATED_DATE AS VARCHAR), '')
        ) AS HASHDIFF,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        CITY,
        COUNTRY,
        CREATED_DATE,
        CURRENT_TIMESTAMP() AS EFFECTIVE_TS,
        CURRENT_TIMESTAMP() AS DWH_VALID_TS,
        'DEMO_DB.DBT_DEV.RAW_CUSTOMERS' AS DSS_RECORD_SOURCE
    FROM DEMO_DB.DBT_DEV.RAW_CUSTOMERS
    WHERE CUSTOMER_ID IS NOT NULL
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY HK_H_CUSTOMERS, HASHDIFF
    ORDER BY DWH_VALID_TS
) = 1;

-- Verify creation
SELECT 'HUB_CUSTOMERS' AS table_name, COUNT(*) AS rows FROM HUB_CUSTOMERS
UNION ALL
SELECT 'SAT_CUSTOMERS_DETAILS', COUNT(*) FROM SAT_CUSTOMERS_DETAILS;

SHOW DYNAMIC TABLES IN SCHEMA DBT_DEV_RAW_VAULT;
