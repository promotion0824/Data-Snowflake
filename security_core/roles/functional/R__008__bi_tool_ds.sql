-- ------------------------------------------------------------------------------------------------------------------------------
-- Create 'bi_tool_ds' functional role
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE USERADMIN;

{% if hasDsSandboxDb -%}

CREATE ROLE IF NOT EXISTS bi_tool_ds;

USE ROLE {{ defaultRole }};
GRANT USAGE ON WAREHOUSE bi_tool_ds_wh TO ROLE bi_tool_ds;
GRANT OPERATE ON WAREHOUSE bi_tool_ds_wh TO ROLE bi_tool_ds;
GRANT ROLE bi_tool_ds TO ROLE SYSADMIN;

{%- endif %}

USE ROLE {{ defaultRole }};