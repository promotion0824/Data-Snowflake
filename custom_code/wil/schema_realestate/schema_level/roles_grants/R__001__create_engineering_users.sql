USE ROLE SECURITYADMIN;
CREATE USER IF NOT EXISTS dhenleymartin
  LOGIN_NAME   = 'dhenley-martin@willowinc.com'
  DISPLAY_NAME = 'Danny'
  FIRST_NAME   = 'Danny'
  LAST_NAME    = 'Henley-Martin' 
  EMAIL        = 'dhenley-martin@willowinc.com'
  DEFAULT_ROLE = engineering
  DEFAULT_WAREHOUSE = analyst_wh
  DEFAULT_NAMESPACE = 'UAT_DB.raw'
  PASSWORD ='';
GRANT ROLE engineering TO USER dhenleymartin;

USE ROLE {{ defaultRole }};
CREATE WAREHOUSE IF NOT EXISTS  analyst_wh WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 3 SCALING_POLICY = 'STANDARD' COMMENT = '';
ALTER WAREHOUSE analyst_wh SUSPEND;
USE ROLE ACCOUNTADMIN;
CREATE RESOURCE MONITOR IF NOT EXISTS  analyst_rm WITH  CREDIT_QUOTA = 50
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 50 PERCENT DO NOTIFY
    		 ON 75 PERCENT DO NOTIFY
             ON 90 PERCENT DO SUSPEND
             ON 100 PERCENT DO SUSPEND_IMMEDIATE;
ALTER WAREHOUSE analyst_wh SET RESOURCE_MONITOR = analyst_rm;
GRANT MONITOR ON WAREHOUSE analyst_wh TO ROLE sysadmin;
USE ROLE {{ defaultRole }};