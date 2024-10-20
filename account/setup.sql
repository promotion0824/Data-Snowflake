-- ******************************************************************************************************************************
-- Initial Snowflake account setup script

-- This script is run immediately after new Snowflake account creation. 
-- This is a pre-requisite for deployment using Schemachange.
-- Note: This script is not driven by Schemachange and it needs to be IDEMPOTENT.
-- ******************************************************************************************************************************

!set variable_substitution=true
!set timing=true
!set echo=true
 
USE ROLE SYSADMIN;

-- ------------------------------------------------------------------------------------------------------------------------------
-- Create database UTIL_DB
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS util_db;

-- ------------------------------------------------------------------------------------------------------------------------------
-- Create schema SCHEMACHANGE
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS util_db.schemachange;

-- ------------------------------------------------------------------------------------------------------------------------------
-- Create schema change_history table for schemachange
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS util_db.schemachange.change_history
(
  version VARCHAR,
  description VARCHAR,
  script VARCHAR,
  script_type VARCHAR,
  checksum VARCHAR,
  execution_time VARCHAR,
  status VARCHAR,
  installed_by VARCHAR,
  installed_on TIMESTAMP_LTZ
);

-- ------------------------------------------------------------------------------------------------------------------------------
-- Create warehouse for deployment pipeline
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS  deployment_pipeline_wh WITH 
    WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 60 
    AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = 1 
    MAX_CLUSTER_COUNT = 1
    SCALING_POLICY = 'ECONOMY' 
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse used for deployment pipeline.'
;

USE ROLE ACCOUNTADMIN;

-- We don't want to suspend the monitoring warehouse automatically
CREATE RESOURCE MONITOR IF NOT EXISTS  deployment_pipeline_rm 
  WITH  
    CREDIT_QUOTA = 50
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 50 PERCENT DO NOTIFY
    		 ON 75 PERCENT DO NOTIFY
             ON 90 PERCENT DO NOTIFY
             ON 100 PERCENT DO NOTIFY;

ALTER WAREHOUSE deployment_pipeline_wh 
SET RESOURCE_MONITOR = deployment_pipeline_rm;

GRANT MONITOR ON WAREHOUSE deployment_pipeline_wh TO ROLE SYSADMIN;

-- ------------------------------------------------------------------------------------------------------------------------------
-- Roles that require elevated permissions to be created
-- ------------------------------------------------------------------------------------------------------------------------------

-- Create a role for global execution monitoring
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS execution_monitor;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE execution_monitor;

-- Global task admin role
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS task_admin;
GRANT EXECUTE TASK, EXECUTE MANAGED TASK ON ACCOUNT TO ROLE task_admin;

-- ------------------------------------------------------------------------------------------------------------------------------
-- Deployment pipeline role
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS deployment_pipeline;

GRANT ROLE deployment_pipeline TO ROLE SYSADMIN;

GRANT USAGE ON WAREHOUSE deployment_pipeline_wh TO ROLE deployment_pipeline;
GRANT OPERATE ON WAREHOUSE deployment_pipeline_wh TO ROLE deployment_pipeline;

GRANT USAGE ON DATABASE util_db TO ROLE deployment_pipeline;
GRANT USAGE ON SCHEMA util_db.public TO ROLE deployment_pipeline;
GRANT USAGE ON SCHEMA util_db.schemachange TO ROLE deployment_pipeline;
GRANT CREATE SCHEMA ON DATABASE util_db TO ROLE deployment_pipeline;
GRANT CREATE TABLE ON SCHEMA util_db.public TO ROLE deployment_pipeline;
GRANT CREATE TABLE ON SCHEMA util_db.schemachange TO ROLE deployment_pipeline;

-- Account level privileges
USE ROLE ACCOUNTADMIN;

GRANT CREATE DATABASE ON ACCOUNT TO ROLE deployment_pipeline;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE deployment_pipeline;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE deployment_pipeline;

-- Required in order to deploy serverless tasks
GRANT ROLE task_admin TO ROLE deployment_pipeline;

USE ROLE SECURITYADMIN;

-- This is needed in order to be able to deploy roles
GRANT ROLE USERADMIN TO ROLE deployment_pipeline;

-- The global MANAGE GRANTS privilege is required to grant privileges on future objects
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE deployment_pipeline;

-- Grant change_history table ownership to deployment_pipeline
GRANT OWNERSHIP ON TABLE util_db.schemachange.change_history
    TO ROLE deployment_pipeline COPY CURRENT GRANTS;

-- ------------------------------------------------------------------------------------------------------------------------------
-- Create user for deployment pipeline
-- ------------------------------------------------------------------------------------------------------------------------------
 
CREATE USER IF NOT EXISTS deployment_pipeline_usr
  LOGIN_NAME   = 'deployment_pipeline_usr'
  DEFAULT_ROLE = deployment_pipeline
  DEFAULT_WAREHOUSE = deployment_pipeline_wh;

-- Every time the pipeline runs, it updates the KV secret
-- If the user already exists we need to also update the password 
ALTER USER deployment_pipeline_usr 
SET PASSWORD = '&deployment_user_password';

GRANT ROLE deployment_pipeline TO USER deployment_pipeline_usr;

-- ------------------------------------------------------------------------------------------------------------------------------
-- Create a role for bulk granting usage on all integrations 
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE ROLE IF NOT EXISTS integrations_user;

GRANT ROLE integrations_user TO ROLE SYSADMIN;

-- ------------------------------------------------------------------------------------------------------------------------------
-- Create database creator role
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS database_creator;

GRANT CREATE DATABASE ON ACCOUNT TO ROLE database_creator;

-- ------------------------------------------------------------------------------------------------------------------------------
-- Set account level resource monitor
-- Default = 1000 credits monthly
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;

CREATE RESOURCE MONITOR IF NOT EXISTS account_rm 
  WITH  
    CREDIT_QUOTA = 1000
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS 
      ON 50 PERCENT DO NOTIFY
    	ON 75 PERCENT DO NOTIFY
      ON 90 PERCENT DO NOTIFY
      ON 100 PERCENT DO NOTIFY;

ALTER ACCOUNT SET RESOURCE_MONITOR = account_rm;

-- ------------------------------------------------------------------------------------------------------------------------------
-- Create a Power BI security integration
-- This is required for Power BI to access Snowflake data through SSO
-- See: https://docs.snowflake.com/en/user-guide/oauth-powerbi#creating-a-power-bi-security-integration
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;

CREATE SECURITY INTEGRATION IF NOT EXISTS powerbi
    type = external_oauth
    enabled = true
    external_oauth_type = azure
    external_oauth_issuer = 'https://sts.windows.net/d43166d1-c2a1-4f26-a213-f620dba13ab8/'
    external_oauth_jws_keys_url = 'https://login.windows.net/common/discovery/keys'
    external_oauth_audience_list = ('https://analysis.windows.net/powerbi/connector/Snowflake', 'https://analysis.windows.net/powerbi/connector/snowflake')
    external_oauth_token_user_mapping_claim = 'upn'
    external_oauth_snowflake_user_mapping_attribute = 'login_name'
;

-- ------------------------------------------------------------------------------------------------------------------------------
-- Configure account to prevent data exfiltration
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- These settings need to be disabled for now as ADF copy activity doesn't work if require storage integration for stage 
-- operation/creation is set to true.See: https://learn.microsoft.com/en-us/azure/data-factory/connector-snowflake?tabs=data-factory#prerequisites
-- We can change these settings if we don't use ADF copy activity anymore.

ALTER ACCOUNT &account_locator 
SET PREVENT_UNLOAD_TO_INLINE_URL = false;

-- ALTER ACCOUNT &account_locator
-- SET REQUIRE_STORAGE_INTEGRATION_FOR_STAGE_CREATION = true;

-- ALTER ACCOUNT &account_locator 
-- SET REQUIRE_STORAGE_INTEGRATION_FOR_STAGE_OPERATION = true;

-- ------------------------------------------------------------------------------------------------------------------------------
-- Set default account timezone to UTC
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN; 

ALTER ACCOUNT 
SET TIMEZONE = 'Etc/UTC';

USE ROLE SYSADMIN;