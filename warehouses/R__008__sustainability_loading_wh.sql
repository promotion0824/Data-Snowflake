-- ------------------------------------------------------------------------------------------------------------------------------
-- Warehouse used for sustainability data loading.
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE {{ defaultRole }};

{% if hasSustainabilityDb -%}

CREATE WAREHOUSE IF NOT EXISTS  sustainability_loading_wh 
  WITH 
    WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 300 
    AUTO_RESUME = TRUE 
    SCALING_POLICY = 'ECONOMY' 
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse used for sustainability data loading.'
;

GRANT OPERATE ON WAREHOUSE sustainability_loading_wh TO ROLE sysadmin;
GRANT MONITOR ON WAREHOUSE sustainability_loading_wh TO ROLE sysadmin;

USE ROLE ACCOUNTADMIN;

CREATE RESOURCE MONITOR IF NOT EXISTS  sustainability_loading_rm 
  WITH  
    CREDIT_QUOTA = 50
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 50 PERCENT DO NOTIFY
    		 ON 75 PERCENT DO NOTIFY
             ON 90 PERCENT DO NOTIFY
             ON 100 PERCENT DO NOTIFY;

ALTER WAREHOUSE sustainability_loading_wh 
SET RESOURCE_MONITOR = sustainability_loading_rm;

GRANT MONITOR ON WAREHOUSE sustainability_loading_wh TO ROLE sysadmin;
{%- endif %}

USE ROLE {{ defaultRole }};

