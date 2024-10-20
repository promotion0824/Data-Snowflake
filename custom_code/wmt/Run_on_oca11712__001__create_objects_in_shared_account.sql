-- ------------------------------------------------------------------------------------------------------------------------------
-- Create main <environment>_db
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS willow_db FROM SHARE WILLOW.WMTEU22.EXTERNAL_SHARE;

CREATE WAREHOUSE IF NOT EXISTS  walmart_ods_wh 
  WITH 
    WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 300 
    AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = 1 
    MAX_CLUSTER_COUNT = 3 
    SCALING_POLICY = 'ECONOMY' 
    INITIALLY_SUSPENDED = TRUE
;

CREATE RESOURCE MONITOR IF NOT EXISTS  walmart_ods_rm 
  WITH  
    CREDIT_QUOTA = 30
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 50 PERCENT DO NOTIFY
    		 ON 75 PERCENT DO NOTIFY
             ON 90 PERCENT DO NOTIFY
             ON 100 PERCENT DO NOTIFY;

ALTER WAREHOUSE walmart_ods_wh SET RESOURCE_MONITOR = walmart_ods_rm;

CREATE ROLE IF NOT EXISTS WMT_READER_ODS;
GRANT MONITOR ON WAREHOUSE walmart_ods_wh TO ROLE WMT_READER_ODS;
GRANT USAGE ON WAREHOUSE walmart_ods_wh TO WMT_READER_ODS;
GRANT IMPORTED PRIVILEGES ON DATABASE willow_db to role WMT_READER_ODS;

GRANT ROLE WMT_READER_ODS TO ROLE ACCOUNTADMIN;