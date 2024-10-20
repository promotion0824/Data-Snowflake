-- ------------------------------------------------------------------------------------------------------------------------------
-- Warehouse used by data engineers
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE {{ defaultRole }};

CREATE WAREHOUSE IF NOT EXISTS  data_engineer_wh 
  WITH 
    WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 300 
    AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = 1 
    MAX_CLUSTER_COUNT = 1
    SCALING_POLICY = 'ECONOMY' 
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse used by data engineers.'
;

GRANT USAGE ON WAREHOUSE data_engineer_wh TO ROLE data_engineer;
GRANT OPERATE ON WAREHOUSE data_engineer_wh TO ROLE data_engineer;
GRANT MODIFY ON WAREHOUSE data_engineer_wh TO ROLE data_engineer;
GRANT MONITOR ON WAREHOUSE data_engineer_wh TO ROLE data_engineer;

GRANT ROLE data_engineer TO ROLE SYSADMIN;

USE ROLE ACCOUNTADMIN;

CREATE RESOURCE MONITOR IF NOT EXISTS  data_engineer_rm 
  WITH  
    CREDIT_QUOTA = 50
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 50 PERCENT DO NOTIFY
    		 ON 75 PERCENT DO NOTIFY
             ON 90 PERCENT DO NOTIFY
             ON 100 PERCENT DO NOTIFY;

ALTER WAREHOUSE data_engineer_wh 
SET RESOURCE_MONITOR = data_engineer_rm;

GRANT MONITOR ON WAREHOUSE data_engineer_wh TO ROLE sysadmin;

USE ROLE {{ defaultRole }};
