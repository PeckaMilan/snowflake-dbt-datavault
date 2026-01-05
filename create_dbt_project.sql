-- Create dbt Project in Snowflake
USE ROLE ACCOUNTADMIN;
USE DATABASE DEMO_DB;
USE SCHEMA DBT_DEV;
USE WAREHOUSE COMPUTE_WH;

-- Fetch latest from Git
ALTER GIT REPOSITORY GIT_REPOS.DBT_PROJECTS.DATAVAULT_DBT_REPO FETCH;

-- List files to verify
LIST @GIT_REPOS.DBT_PROJECTS.DATAVAULT_DBT_REPO/branches/main/;

-- Create dbt project from Git repository stage path
CREATE OR REPLACE DBT PROJECT datavault_customers
    FROM '@GIT_REPOS.DBT_PROJECTS.DATAVAULT_DBT_REPO/branches/main';

-- Verify
SHOW DBT PROJECTS;
