-- ------------------------------------------------------------------------------------------------------------------------------
-- Warehouse used by data scientists (Snowpark optimized)
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE {{ defaultRole }};

CREATE WAREHOUSE IF NOT EXISTS  data_scientist_snowpark_wh 
  WITH 
    WAREHOUSE_SIZE = 'MEDIUM' -- Snowpark-optimized warehouses are not supported on X-SMALL or SMALL warehouse sizes.
    WAREHOUSE_TYPE = 'SNOWPARK-OPTIMIZED'
    AUTO_SUSPEND = 300 
    AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = 1 
    MAX_CLUSTER_COUNT = 1
    SCALING_POLICY = 'ECONOMY' 
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse used by data scientists (Snowpark optimized).'
;

GRANT OPERATE ON WAREHOUSE data_scientist_snowpark_wh TO ROLE sysadmin;
GRANT MONITOR ON WAREHOUSE data_scientist_snowpark_wh TO ROLE sysadmin;

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

ALTER WAREHOUSE data_scientist_snowpark_wh 
SET RESOURCE_MONITOR = data_scientist_rm;

GRANT MONITOR ON WAREHOUSE data_scientist_snowpark_wh TO ROLE sysadmin;

USE ROLE {{ defaultRole }};
