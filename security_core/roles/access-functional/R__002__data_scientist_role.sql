-- ------------------------------------------------------------------------------------------------------------------------------
-- Grant Data Science Sandbox DB access roles to 'data_scientist' functional role
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE USERADMIN;

{% if hasDsSandboxDb -%}

GRANT ROLE ds_sandbox_all TO ROLE data_scientist;

{%- endif %}

USE ROLE {{ defaultRole }};