-- ------------------------------------------------------------------------------------------------------------------------------
-- Create custom warehouses and resource monitors
-- ------------------------------------------------------------------------------------------------------------------------------

-- Create Warehouse
USE ROLE {{ defaultRole }};

CREATE WAREHOUSE IF NOT EXISTS  segment_loading_wh 
  WITH 
    WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 300 
    AUTO_RESUME = TRUE 
    SCALING_POLICY = 'ECONOMY' 
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse used for Segment data loading.'
;

USE ROLE ACCOUNTADMIN;

-- we don't want to suspend the monitoring warehouse automatically
CREATE RESOURCE MONITOR IF NOT EXISTS  segment_loading_rm 
  WITH  
    CREDIT_QUOTA = 50
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 50 PERCENT DO NOTIFY
    		 ON 75 PERCENT DO NOTIFY
             ON 90 PERCENT DO NOTIFY
             ON 100 PERCENT DO NOTIFY;

ALTER WAREHOUSE segment_loading_wh 
SET RESOURCE_MONITOR = segment_loading_rm;

GRANT MONITOR ON WAREHOUSE segment_loading_wh TO ROLE sysadmin;

-- ------------------------------------------------------------------------------------------------------------------------------

-- Create Warehouse
USE ROLE {{ defaultRole }};

CREATE WAREHOUSE IF NOT EXISTS  weather_loading_wh 
  WITH 
    WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 300 
    AUTO_RESUME = TRUE 
    SCALING_POLICY = 'ECONOMY' 
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse used for weather data loading.'
;

USE ROLE ACCOUNTADMIN;

-- we don't want to suspend the monitoring warehouse automatically
CREATE RESOURCE MONITOR IF NOT EXISTS  weather_loading 
  WITH  
    CREDIT_QUOTA = 50
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 50 PERCENT DO NOTIFY
    		 ON 75 PERCENT DO NOTIFY
             ON 90 PERCENT DO NOTIFY
             ON 100 PERCENT DO NOTIFY;

ALTER WAREHOUSE weather_loading_wh 
SET RESOURCE_MONITOR = weather_loading;

GRANT MONITOR ON WAREHOUSE weather_loading_wh TO ROLE sysadmin;


USE ROLE {{ defaultRole }};
