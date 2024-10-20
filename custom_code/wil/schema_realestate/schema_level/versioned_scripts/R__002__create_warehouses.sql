-- ------------------------------------------------------------------------------------------------------------------------------
-- Create environment warehouses and the resource monitors
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE WAREHOUSE IF NOT EXISTS  wil_automation_wh WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 3 SCALING_POLICY = 'STANDARD' COMMENT = '';
--ALTER WAREHOUSE wil_automation_wh SUSPEND;

USE ROLE ACCOUNTADMIN;

CREATE RESOURCE MONITOR IF NOT EXISTS  wil_automation_rm WITH  CREDIT_QUOTA = 50
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 50 PERCENT DO NOTIFY
    		 ON 75 PERCENT DO NOTIFY
             ON 90 PERCENT DO SUSPEND
             ON 100 PERCENT DO SUSPEND_IMMEDIATE;
             
ALTER WAREHOUSE wil_automation_wh 
SET RESOURCE_MONITOR = wil_automation_rm;

GRANT MONITOR ON WAREHOUSE wil_automation_wh TO ROLE sysadmin;

USE ROLE {{ defaultRole }};