-- ------------------------------------------------------------------------------------------------------------------------------
-- Create Users and roles
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE SECURITYADMIN;
CREATE USER IF NOT EXISTS monitoring_dashboard_usr
  LOGIN_NAME   = 'monitoring_dashboard_usr'
  DEFAULT_ROLE = monitoring_dashboard
  DEFAULT_WAREHOUSE = monitoring_dashboard_wh
  DEFAULT_NAMESPACE = 'monitoring_db.published'
  PASSWORD = '';   -- deploy manually with real password (stored in lastpass); 
  -- DON'T CHECK IN the password.

CREATE ROLE IF NOT EXISTS monitoring_dashboard;
GRANT ROLE monitoring_dashboard TO USER monitoring_dashboard_usr;

GRANT USAGE ON DATABASE monitoring_db TO ROLE monitoring_dashboard;
GRANT USAGE ON SCHEMA monitoring_db.raw TO ROLE monitoring_dashboard;
GRANT USAGE ON SCHEMA monitoring_db.transformed TO ROLE monitoring_dashboard;
GRANT USAGE ON SCHEMA monitoring_db.published TO ROLE monitoring_dashboard;

USE ROLE ACCOUNTADMIN;
GRANT SELECT ON ALL tables IN SCHEMA monitoring_db.published TO ROLE monitoring_dashboard;
GRANT SELECT ON ALL views  IN SCHEMA monitoring_db.published TO ROLE monitoring_dashboard;
GRANT SELECT ON future tables IN SCHEMA monitoring_db.published TO ROLE monitoring_dashboard;
GRANT SELECT ON future views  IN SCHEMA monitoring_db.published TO ROLE monitoring_dashboard;

GRANT SELECT ON ALL tables IN SCHEMA monitoring_db.transformed TO ROLE monitoring_dashboard;
GRANT SELECT ON ALL views  IN SCHEMA monitoring_db.transformed TO ROLE monitoring_dashboard;
GRANT SELECT ON future tables IN SCHEMA monitoring_db.transformed TO ROLE monitoring_dashboard;
GRANT SELECT ON future views  IN SCHEMA monitoring_db.transformed TO ROLE monitoring_dashboard;

GRANT SELECT ON ALL tables IN SCHEMA monitoring_db.raw TO ROLE monitoring_dashboard;
GRANT SELECT ON ALL views  IN SCHEMA monitoring_db.raw TO ROLE monitoring_dashboard;
GRANT SELECT ON future tables IN SCHEMA monitoring_db.raw TO ROLE monitoring_dashboard;
GRANT SELECT ON future views  IN SCHEMA monitoring_db.raw TO ROLE monitoring_dashboard;

GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE monitoring_dashboard;

------------------------------------------------------------------------------------------
-- Create Warehouse
USE ROLE {{ defaultRole }};
CREATE WAREHOUSE IF NOT EXISTS  monitoring_dashboard_wh WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 3 SCALING_POLICY = 'STANDARD' COMMENT = ''
--; ALTER WAREHOUSE monitoring_dashboard_wh SUSPEND
;

USE ROLE ACCOUNTADMIN;
-- we don't want to suspend the monitoring warehouse automatically
CREATE RESOURCE MONITOR IF NOT EXISTS  monitoring_dashboard_rm WITH  CREDIT_QUOTA = 50
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 50 PERCENT DO NOTIFY
    		 ON 75 PERCENT DO NOTIFY
             ON 90 PERCENT DO NOTIFY
             ON 100 PERCENT DO NOTIFY;

ALTER WAREHOUSE monitoring_dashboard_wh SET RESOURCE_MONITOR = monitoring_dashboard_rm;
GRANT MONITOR ON WAREHOUSE monitoring_dashboard_wh TO ROLE sysadmin;
GRANT MONITOR ON WAREHOUSE monitoring_dashboard_wh TO ROLE monitoring_dashboard;
USE ROLE {{ defaultRole }};


GRANT USAGE ON warehouse monitoring_dashboard_wh TO ROLE monitoring_dashboard;
GRANT OPERATE ON warehouse monitoring_dashboard_wh TO ROLE monitoring_dashboard;

-- To see the cost for the account
USE ROLE ACCOUNTADMIN;
GRANT MONITOR USAGE ON ACCOUNT TO ROLE monitoring_dashboard;
USE ROLE {{ defaultRole }};

GRANT USAGE ON DATABASE CENTRAL_MONITORING_DB TO ROLE MONITORING_DASHBOARD;
GRANT ALL ON SCHEMA CENTRAL_MONITORING_DB.PUBLIC TO ROLE MONITORING_DASHBOARD;
