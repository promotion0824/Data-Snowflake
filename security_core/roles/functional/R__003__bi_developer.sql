-- ------------------------------------------------------------------------------------------------------------------------------
-- Create 'bi_developer' functional role
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS bi_developer;

USE ROLE {{ defaultRole }};

GRANT USAGE ON WAREHOUSE bi_developer_wh TO ROLE bi_developer;
GRANT OPERATE ON WAREHOUSE bi_developer_wh TO ROLE bi_developer;

-- Usage on dummy _<customerAbbrv> database 
GRANT USAGE ON DATABASE _{{ customerAbbrv }} TO ROLE bi_developer;

GRANT IMPORTED PRIVILEGES ON DATABASE Snowflake TO ROLE bi_developer;

GRANT DATABASE ROLE SNOWFLAKE.GOVERNANCE_VIEWER TO ROLE bi_developer;
GRANT MONITOR ON warehouse sigma_prd_wh TO ROLE bi_developer;
GRANT MONITOR ON warehouse app_dashboards_prd_wh TO ROLE bi_developer;

GRANT ROLE bi_developer TO ROLE SYSADMIN;

USE ROLE {{ defaultRole }};