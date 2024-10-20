-- ------------------------------------------------------------------------------------------------------------------------------
-- Generic <environment> warehouse
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE {{ defaultRole }};

-- Set warehouse configuration values based on <environment>
{% if environment|lower == 'prd' %}

{% set warehouse_size = 'XSMALL' %}
{% set scaling_policy = 'STANDARD' %}
{% set min_cluster_count = 1 %}
{% set max_cluster_count = 3 %}
{% set auto_suspend = 300 %}
{% set rm_credit_quota = 50 %}
{% set rm_frequency = 'MONTHLY' %}
{% set rm_50pct_action = 'NOTIFY' %}
{% set rm_75pct_action = 'NOTIFY' %}
{% set rm_90pct_action = 'NOTIFY' %}
{% set rm_100pct_action = 'NOTIFY' %}

{% elif environment|lower == 'uat' %}

{% set warehouse_size = 'XSMALL' %}
{% set scaling_policy = 'STANDARD' %}
{% set min_cluster_count = 1 %}
{% set max_cluster_count = 3 %}
{% set auto_suspend = 300 %}
{% set rm_credit_quota = 50 %}
{% set rm_frequency = 'MONTHLY' %}
{% set rm_50pct_action = 'NOTIFY' %}
{% set rm_75pct_action = 'NOTIFY' %}
{% set rm_90pct_action = 'NOTIFY' %}
{% set rm_100pct_action = 'NOTIFY' %}

{% else %}

{% set warehouse_size = 'XSMALL' %}
{% set scaling_policy = 'STANDARD' %}
{% set min_cluster_count = 1 %}
{% set max_cluster_count = 3 %}
{% set auto_suspend = 300 %}
{% set rm_credit_quota = 50 %}
{% set rm_frequency = 'MONTHLY' %}
{% set rm_50pct_action = 'NOTIFY' %}
{% set rm_75pct_action = 'NOTIFY' %}
{% set rm_90pct_action = 'NOTIFY' %}
{% set rm_100pct_action = 'NOTIFY' %}

{% endif %}

CREATE WAREHOUSE IF NOT EXISTS app_dashboards_{{ environment|lower }}_wh 
  WITH 
    WAREHOUSE_SIZE = {{ warehouse_size }}
    AUTO_SUSPEND = {{ auto_suspend }} 
    AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = {{ min_cluster_count }}
    MAX_CLUSTER_COUNT = {{ max_cluster_count }}
    SCALING_POLICY = {{ scaling_policy }}
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Generic {{ environment|upper }} warehouse.'
;

GRANT OPERATE ON WAREHOUSE app_dashboards_{{ environment|lower }}_wh TO ROLE SYSADMIN;
GRANT MONITOR ON WAREHOUSE app_dashboards_{{ environment|lower }}_wh TO ROLE SYSADMIN;

USE ROLE ACCOUNTADMIN;

CREATE RESOURCE MONITOR IF NOT EXISTS  app_dashboards_{{ environment|lower }}_rm 
  WITH  
    CREDIT_QUOTA = {{ rm_credit_quota }}
    FREQUENCY = {{ rm_frequency }}
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS 
      ON 50 PERCENT DO {{ rm_50pct_action }}
    	ON 75 PERCENT DO {{ rm_75pct_action }}
      ON 90 PERCENT DO {{ rm_90pct_action }}
      ON 100 PERCENT DO {{ rm_100pct_action }};

ALTER WAREHOUSE app_dashboards_{{ environment|lower }}_wh 
SET RESOURCE_MONITOR = app_dashboards_{{ environment|lower }}_rm;

GRANT MONITOR ON WAREHOUSE app_dashboards_{{ environment|lower }}_wh TO ROLE SYSADMIN;

USE ROLE {{ defaultRole }};