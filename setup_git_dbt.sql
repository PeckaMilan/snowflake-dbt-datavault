-- ============================================================
-- SETUP GIT INTEGRATION + DBT PROJECT FOR MULTI-TABLE MODEL
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE DEMO_DB;

-- 1. Create schema for Git repos
CREATE SCHEMA IF NOT EXISTS GIT_REPOS;

-- 2. Create API integration for GitHub (if not exists)
CREATE OR REPLACE API INTEGRATION github_api_integration
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/PeckaMilan/')
    ENABLED = TRUE;

-- 3. Create Git repository
CREATE OR REPLACE GIT REPOSITORY GIT_REPOS.DATAVAULT_REPO
    API_INTEGRATION = github_api_integration
    ORIGIN = 'https://github.com/PeckaMilan/snowflake-dbt-datavault.git';

-- 4. Fetch latest
ALTER GIT REPOSITORY GIT_REPOS.DATAVAULT_REPO FETCH;

-- 5. List branches
SHOW GIT BRANCHES IN GIT_REPOSITORY GIT_REPOS.DATAVAULT_REPO;

-- 6. List files
LIST @GIT_REPOS.DATAVAULT_REPO/branches/main/;
