-- ------------------------------------------------------------------------------------------------------------------------------
-- pbi <environment> reporting warehouse
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

CREATE OR REPLACE WAREHOUSE  pbi_{{ environment|lower }}_wh 
  WITH 
    WAREHOUSE_SIZE = {{ warehouse_size }}
    AUTO_SUSPEND = {{ auto_suspend }} 
    AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = {{ min_cluster_count }}
    MAX_CLUSTER_COUNT = {{ max_cluster_count }}
    SCALING_POLICY = {{ scaling_policy }}
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse used for pbi reports in {{ environment|upper }}.'
;

GRANT OPERATE ON WAREHOUSE pbi_{{ environment|lower }}_wh TO ROLE SYSADMIN;
GRANT MONITOR ON WAREHOUSE pbi_{{ environment|lower }}_wh TO ROLE SYSADMIN;


GRANT USAGE ON DATABASE analytics_db TO ROLE bi_tool_prd;
GRANT USAGE ON SCHEMA analytics_db.public TO ROLE bi_tool_prd;

GRANT SELECT ON ALL TABLES  IN SCHEMA analytics_db.public TO ROLE bi_tool_prd;
GRANT SELECT ON FUTURE TABLES  IN SCHEMA analytics_db.public TO ROLE bi_tool_prd;
GRANT SELECT ON ALL VIEWS  IN SCHEMA analytics_db.public TO ROLE bi_tool_prd;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA analytics_db.public TO ROLE bi_tool_prd;

