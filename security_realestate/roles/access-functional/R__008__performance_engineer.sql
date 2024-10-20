-- ------------------------------------------------------------------------------------------------------------------------------
-- Grant environment specific access roles to 'performance_engineer' functional role
-- ------------------------------------------------------------------------------------------------------------------------------

GRANT CREATE TABLE ON SCHEMA ANALYTICS_DB.PUBLIC TO ROLE performance_engineer;
USE ROLE USERADMIN;
{% if environment == 'uat' or environment == 'prd' -%}

GRANT ROLE {{ environment }}_published_r TO ROLE performance_engineer;
GRANT ROLE {{ environment }}_transformed_r TO ROLE performance_engineer;

{%- endif %}

USE ROLE {{ defaultRole }};