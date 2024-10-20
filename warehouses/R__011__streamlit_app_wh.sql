-- ------------------------------------------------------------------------------------------------------------------------------
-- Warehouse used for Streamlit Apps
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE {{ defaultRole }};

CREATE WAREHOUSE IF NOT EXISTS  streamlit_app_wh 
  WITH 
    WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 1800 -- Setting this value to 30 minutes to prevent auto-suspension while the app is in use for customer demos
    AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = 1 
    MAX_CLUSTER_COUNT = 1
    SCALING_POLICY = 'ECONOMY' 
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse used for Streamlit Apps'
;

GRANT OPERATE ON WAREHOUSE streamlit_app_wh TO ROLE sysadmin;
GRANT MONITOR ON WAREHOUSE streamlit_app_wh TO ROLE sysadmin;

USE ROLE ACCOUNTADMIN;

CREATE RESOURCE MONITOR IF NOT EXISTS  streamlit_app_rm 
  WITH  
    CREDIT_QUOTA = 50
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 50 PERCENT DO NOTIFY
    		 ON 75 PERCENT DO NOTIFY
             ON 90 PERCENT DO NOTIFY
             ON 100 PERCENT DO NOTIFY;

ALTER WAREHOUSE streamlit_app_wh 
SET RESOURCE_MONITOR = streamlit_app_rm;

GRANT MONITOR ON WAREHOUSE streamlit_app_wh TO ROLE sysadmin;

USE ROLE {{ defaultRole }};
