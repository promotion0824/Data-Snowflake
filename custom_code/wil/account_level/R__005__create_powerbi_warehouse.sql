-- ------------------------------------------------------------------------------------------------------------------------------
-- Create warehouses and the resource monitors for reporting tools
-- ------------------------------------------------------------------------------------------------------------------------------

-- Create Warehouses
USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS  bitool_powerbi_wh 
  WITH 
    WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 300 
    AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = 1 
    MAX_CLUSTER_COUNT = 3 
    SCALING_POLICY = 'STANDARD'
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse used for PowerBI evaluation.'
;

-- Create Resource Monitors
USE ROLE ACCOUNTADMIN;

CREATE RESOURCE MONITOR IF NOT EXISTS  bitool_powerbi_rm 
  WITH  
    CREDIT_QUOTA = 50
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS 
      ON 50 PERCENT DO NOTIFY
      ON 75 PERCENT DO NOTIFY
      ON 90 PERCENT DO NOTIFY
      ON 100 PERCENT DO NOTIFY;

ALTER WAREHOUSE bitool_powerbi_wh 
SET RESOURCE_MONITOR = bitool_powerbi_rm;

GRANT MONITOR ON WAREHOUSE bitool_powerbi_wh TO ROLE sysadmin;

USE ROLE SYSADMIN;
