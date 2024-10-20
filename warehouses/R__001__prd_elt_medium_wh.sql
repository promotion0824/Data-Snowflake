-- ------------------------------------------------------------------------------------------------------------------------------
-- Generic compute warehouse
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE {{ defaultRole }};

CREATE WAREHOUSE IF NOT EXISTS  prd_elt_medium_wh WITH 
    WAREHOUSE_SIZE = 'MEDIUM' 
    AUTO_SUSPEND = 1 
    AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = 1 
    MAX_CLUSTER_COUNT = 1
    SCALING_POLICY = 'ECONOMY' 
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Generic compute warehouse.'
;

GRANT OPERATE ON WAREHOUSE prd_elt_medium_wh TO ROLE SYSADMIN;
USE ROLE ACCOUNTADMIN;

CREATE RESOURCE MONITOR IF NOT EXISTS  sysadmin_compute_rm 
  WITH  
    CREDIT_QUOTA = 50
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS 
      ON 50 PERCENT DO NOTIFY
    	ON 75 PERCENT DO NOTIFY
      ON 90 PERCENT DO NOTIFY
      ON 100 PERCENT DO NOTIFY;

ALTER WAREHOUSE prd_elt_medium_wh 
SET RESOURCE_MONITOR = sysadmin_compute_rm;

GRANT MONITOR ON WAREHOUSE prd_elt_medium_wh TO ROLE SYSADMIN;

USE ROLE {{ defaultRole }};

