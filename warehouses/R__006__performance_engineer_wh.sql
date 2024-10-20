-- ------------------------------------------------------------------------------------------------------------------------------
-- Warehouse used by performance engineers
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE {{ defaultRole }};

CREATE WAREHOUSE IF NOT EXISTS  performance_engineer_wh 
  WITH 
    WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 300 
    AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = 1 
    MAX_CLUSTER_COUNT = 1
    SCALING_POLICY = 'ECONOMY' 
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse used by performance engineers.'
;

GRANT OPERATE ON WAREHOUSE performance_engineer_wh TO ROLE sysadmin;
GRANT MONITOR ON WAREHOUSE performance_engineer_wh TO ROLE sysadmin;

USE ROLE ACCOUNTADMIN;

CREATE RESOURCE MONITOR IF NOT EXISTS  performance_engineer_rm 
  WITH  
    CREDIT_QUOTA = 50
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 50 PERCENT DO NOTIFY
    		 ON 75 PERCENT DO NOTIFY
             ON 90 PERCENT DO NOTIFY
             ON 100 PERCENT DO NOTIFY;

ALTER WAREHOUSE performance_engineer_wh 
SET RESOURCE_MONITOR = performance_engineer_rm;

GRANT MONITOR ON WAREHOUSE performance_engineer_wh TO ROLE sysadmin;

USE ROLE {{ defaultRole }};
