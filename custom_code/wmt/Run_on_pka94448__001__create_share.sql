-- ------------------------------------------------------------------------------------------------------------------------------
-- Create main <environment>_db
-- ------------------------------------------------------------------------------------------------------------------------------


USE ROLE ACCOUNTADMIN;
CREATE SHARE IF NOT EXISTS external_share;
ALTER SHARE external_share ADD ACCOUNTS = WILLOW.WALMART_CORPORATE;

USE ROLE SYSADMIN;
GRANT USAGE ON DATABASE prd_db TO SHARE external_share;
GRANT USAGE ON SCHEMA prd_db.published TO SHARE external_share;




