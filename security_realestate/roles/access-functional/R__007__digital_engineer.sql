-- ------------------------------------------------------------------------------------------------------------------------------
-- Grant environment specific access roles to 'digital_engineer' functional role
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;
{% if environment == 'prd' -%}

GRANT ROLE {{ environment }}_published_r TO ROLE digital_engineer;

{%- endif %}

USE ROLE {{ defaultRole }};