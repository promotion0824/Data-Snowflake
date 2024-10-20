-- ------------------------------------------------------------------------------------------------------------------------------
-- Sigma <environment> reporting warehouse
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE {{ defaultRole }};

-- Set warehouse configuration values based on <environment>
{% if environment|lower == 'prd' %}

{% set warehouse_size = 'XSMALL' %}
{% set scaling_policy = 'STANDARD' %}
{% set min_cluster_count = 1 %}
{% set max_cluster_count = 3 %}
{% set auto_suspend = 300 %}
{% set credit_quota = 100 %}

{% elif environment|lower == 'uat' %}

{% set warehouse_size = 'XSMALL' %}
{% set scaling_policy = 'STANDARD' %}
{% set min_cluster_count = 1 %}
{% set max_cluster_count = 3 %}
{% set auto_suspend = 300 %}
{% set credit_quota = 50 %}

{% else %}

{% set warehouse_size = 'XSMALL' %}
{% set scaling_policy = 'STANDARD' %}
{% set min_cluster_count = 1 %}
{% set max_cluster_count = 1 %}
{% set auto_suspend = 300 %}
{% set credit_quota = 50 %}

{% endif %}

CREATE WAREHOUSE IF NOT EXISTS sigma_{{ environment|lower }}_wh 
  WITH 
    WAREHOUSE_SIZE = {{ warehouse_size }}
    AUTO_SUSPEND = {{ auto_suspend }} 
    AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = {{ min_cluster_count }}
    MAX_CLUSTER_COUNT = {{ max_cluster_count }}
    SCALING_POLICY = {{ scaling_policy }}
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse used for Sigma reports in {{ environment|upper }}.'
;
USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS bi_tool_{{ environment }};

USE ROLE {{ defaultRole }};
GRANT USAGE ON warehouse sigma_{{ environment|lower }}_wh TO ROLE bi_tool_{{ environment|lower }};
GRANT OPERATE ON warehouse sigma_{{ environment|lower }}_wh TO ROLE bi_tool_{{ environment|lower }};

GRANT OPERATE ON WAREHOUSE sigma_{{ environment|lower }}_wh TO ROLE SYSADMIN;
GRANT MONITOR ON WAREHOUSE sigma_{{ environment|lower }}_wh TO ROLE SYSADMIN;

USE ROLE ACCOUNTADMIN;

CREATE RESOURCE MONITOR IF NOT EXISTS  sigma_{{ environment|lower }}_rm 
  WITH  
    CREDIT_QUOTA = {{ credit_quota }}
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS 
      ON 50 PERCENT DO NOTIFY
    	ON 75 PERCENT DO NOTIFY
      ON 90 PERCENT DO NOTIFY
      ON 100 PERCENT DO NOTIFY;

ALTER WAREHOUSE sigma_{{ environment|lower }}_wh 
SET RESOURCE_MONITOR = sigma_{{ environment|lower }}_rm;

GRANT MONITOR ON WAREHOUSE sigma_{{ environment|lower }}_wh TO ROLE SYSADMIN;

USE ROLE {{ defaultRole }};

