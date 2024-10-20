-- ------------------------------------------------------------------------------------------------------------------------------
-- Grant environment specific access roles to 'performance_engineer' functional role
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;
{% if environment == 'uat' or environment == 'prd' -%}

GRANT ROLE {{ environment }}_published_r TO ROLE performance_engineer;
GRANT ROLE {{ environment }}_transformed_r TO ROLE performance_engineer;
GRANT ROLE {{ environment }}_raw_r TO ROLE performance_engineer;
{%- endif %}


USE ROLE {{ defaultRole }};