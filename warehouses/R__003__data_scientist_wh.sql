-- ------------------------------------------------------------------------------------------------------------------------------
-- Warehouse used by data scientists
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE {{ defaultRole }};

CREATE WAREHOUSE IF NOT EXISTS  data_scientist_wh 
  WITH 
    WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 300 
    AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = 1 
    MAX_CLUSTER_COUNT = 1
    SCALING_POLICY = 'ECONOMY' 
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse used by data scientists.'
;

GRANT OPERATE ON WAREHOUSE data_scientist_wh TO ROLE sysadmin;
GRANT MONITOR ON WAREHOUSE data_scientist_wh TO ROLE sysadmin;

USE ROLE ACCOUNTADMIN;

CREATE RESOURCE MONITOR IF NOT EXISTS  data_scientist_rm 
  WITH  
    CREDIT_QUOTA = 50
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 50 PERCENT DO NOTIFY
    		 ON 75 PERCENT DO NOTIFY
             ON 90 PERCENT DO NOTIFY
             ON 100 PERCENT DO NOTIFY;

ALTER WAREHOUSE data_scientist_wh 
SET RESOURCE_MONITOR = data_scientist_rm;

GRANT MONITOR ON WAREHOUSE data_scientist_wh TO ROLE sysadmin;

USE ROLE {{ defaultRole }};
