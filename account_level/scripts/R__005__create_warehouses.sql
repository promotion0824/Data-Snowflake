
-- ------------------------------------------------------------------------------------------------------------------------------
-- Monitoring dashboard warehouse
-- ------------------------------------------------------------------------------------------------------------------------------
-- Deploy only for Willow AU internal account
{% if accountType == 'internal' and customerName == 'wil' and azureRegionIdentifier == 'aue1' -%}

-- Create Warehouse
USE ROLE {{ defaultRole }};

CREATE WAREHOUSE IF NOT EXISTS  monitoring_dashboard_wh 
  WITH 
    WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 300 
    AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = 1 
    MAX_CLUSTER_COUNT = 3 
    SCALING_POLICY = 'ECONOMY' 
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse used for Sigma monitoring dashboards.'
;

USE ROLE ACCOUNTADMIN;

-- we don't want to suspend the monitoring warehouse automatically
CREATE RESOURCE MONITOR IF NOT EXISTS  monitoring_dashboard_rm 
  WITH  
    CREDIT_QUOTA = 50
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 50 PERCENT DO NOTIFY
    		 ON 75 PERCENT DO NOTIFY
             ON 90 PERCENT DO NOTIFY
             ON 100 PERCENT DO NOTIFY;

ALTER WAREHOUSE monitoring_dashboard_wh 
SET RESOURCE_MONITOR = monitoring_dashboard_rm;

GRANT MONITOR ON WAREHOUSE monitoring_dashboard_wh TO ROLE sysadmin;

USE ROLE {{ defaultRole }};

{%- endif %}

SELECT 1;