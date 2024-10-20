-- ------------------------------------------------------------------------------------------------------------------------------
-- Warehouse used for monitoring_pipeline and logs pipeline.
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE {{ defaultRole }};

{% if hasMonitoringDb -%}

CREATE WAREHOUSE IF NOT EXISTS  monitoring_pipeline_wh WITH 
    WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 60 
    AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = 1 
    MAX_CLUSTER_COUNT = 1
    SCALING_POLICY = 'ECONOMY' 
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse used for monitoring_pipeline and logs pipeline.'
;

GRANT OPERATE ON WAREHOUSE monitoring_pipeline_wh TO ROLE sysadmin;
GRANT MONITOR ON WAREHOUSE monitoring_pipeline_wh TO ROLE sysadmin;

USE ROLE ACCOUNTADMIN;

CREATE RESOURCE MONITOR IF NOT EXISTS  monitoring_pipeline_rm  
  WITH  
    CREDIT_QUOTA = 50
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 50 PERCENT DO NOTIFY
    		 ON 75 PERCENT DO NOTIFY
             ON 90 PERCENT DO NOTIFY
             ON 100 PERCENT DO NOTIFY;

ALTER WAREHOUSE monitoring_pipeline_wh 
SET RESOURCE_MONITOR = monitoring_pipeline_rm;

GRANT MONITOR ON WAREHOUSE monitoring_pipeline_wh TO ROLE sysadmin;
{%- endif %}

USE ROLE {{ defaultRole }};

