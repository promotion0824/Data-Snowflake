-- ------------------------------------------------------------------------------------------------------------------------------
-- Grant environment specific access roles to 'analyst' functional role
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;
{% if environment == 'prd' -%}

GRANT ROLE {{ environment }}_published_r TO ROLE analyst;

{%- endif %}

USE ROLE {{ defaultRole }};