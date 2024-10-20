-- ------------------------------------------------------------------------------------------------------------------------------
-- Warehouse used by BI developers
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE {{ defaultRole }};

CREATE WAREHOUSE IF NOT EXISTS  bi_developer_wh 
  WITH 
    WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 300 
    AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = 1 
    MAX_CLUSTER_COUNT = 1
    SCALING_POLICY = 'ECONOMY' 
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse used by BI developers.'
;

GRANT OPERATE ON WAREHOUSE bi_developer_wh TO ROLE sysadmin;
GRANT MONITOR ON WAREHOUSE bi_developer_wh TO ROLE sysadmin;

USE ROLE ACCOUNTADMIN;

CREATE RESOURCE MONITOR IF NOT EXISTS  bi_developer_rm 
  WITH  
    CREDIT_QUOTA = 50
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 50 PERCENT DO NOTIFY
    		 ON 75 PERCENT DO NOTIFY
             ON 90 PERCENT DO NOTIFY
             ON 100 PERCENT DO NOTIFY;

ALTER WAREHOUSE bi_developer_wh 
SET RESOURCE_MONITOR = bi_developer_rm;

GRANT MONITOR ON WAREHOUSE bi_developer_wh TO ROLE sysadmin;

USE ROLE {{ defaultRole }};
