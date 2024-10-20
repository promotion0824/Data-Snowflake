-- ------------------------------------------------------------------------------------------------------------------------------
-- Warehouse used for BI tools to present results of Data Science work.
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE {{ defaultRole }};

{% if hasDsSandboxDb -%}

CREATE WAREHOUSE IF NOT EXISTS  bi_tool_ds_wh WITH 
    WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 60 
    AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = 1 
    MAX_CLUSTER_COUNT = 1
    SCALING_POLICY = 'ECONOMY' 
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse used for BI tools to present results of Data Science work.'
;

GRANT OPERATE ON WAREHOUSE bi_tool_ds_wh TO ROLE sysadmin;
GRANT MONITOR ON WAREHOUSE bi_tool_ds_wh TO ROLE sysadmin;

USE ROLE ACCOUNTADMIN;

CREATE RESOURCE MONITOR IF NOT EXISTS  bi_tool_ds_rm  
  WITH  
    CREDIT_QUOTA = 50
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 50 PERCENT DO NOTIFY
    		 ON 75 PERCENT DO NOTIFY
             ON 90 PERCENT DO NOTIFY
             ON 100 PERCENT DO NOTIFY;

ALTER WAREHOUSE bi_tool_ds_wh 
SET RESOURCE_MONITOR = bi_tool_ds_rm;

{%- endif %}

USE ROLE {{ defaultRole }};

