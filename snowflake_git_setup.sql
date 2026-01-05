-- ============================================================================
-- Snowflake Git Integration Setup for dbt
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- 1. Create API Integration for GitHub (public repos don't need secrets)
CREATE OR REPLACE API INTEGRATION git_api_integration_public
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/PeckaMilan/')
    ENABLED = TRUE;

-- 2. Create database for Git repositories (if not exists)
CREATE DATABASE IF NOT EXISTS GIT_REPOS;
CREATE SCHEMA IF NOT EXISTS GIT_REPOS.DBT_PROJECTS;

USE DATABASE GIT_REPOS;
USE SCHEMA DBT_PROJECTS;

-- 3. Create Git Repository object
CREATE OR REPLACE GIT REPOSITORY datavault_dbt_repo
    API_INTEGRATION = git_api_integration_public
    ORIGIN = 'https://github.com/PeckaMilan/snowflake-dbt-datavault.git';

-- 4. Verify repository
SHOW GIT REPOSITORIES;

-- 5. List branches
ALTER GIT REPOSITORY datavault_dbt_repo FETCH;
SHOW GIT BRANCHES IN datavault_dbt_repo;

-- 6. List files in the repository
LIST @datavault_dbt_repo/branches/main/;

-- ============================================================================
-- Now go to Snowsight UI to create dbt Project:
-- Data > dbt Projects > + dbt Project
--
-- Settings:
-- - Name: datavault_customers
-- - Git Repository: GIT_REPOS.DBT_PROJECTS.DATAVAULT_DBT_REPO
-- - Branch: main
-- - Subdirectory: (leave empty - root)
-- - Warehouse: COMPUTE_WH
-- - Database: DEMO_DB
-- - Schema: DBT_DEV
-- ============================================================================
