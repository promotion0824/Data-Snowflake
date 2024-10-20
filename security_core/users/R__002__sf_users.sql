-- ------------------------------------------------------------------------------------------------------------------------------
-- Create Snowflake (non-AAD backed) users
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;

{% if hasSustainabilityDb -%}

CREATE USER IF NOT EXISTS sustainability_loading_usr
  LOGIN_NAME   = 'sustainability_loading_usr'
  DEFAULT_ROLE = sustainability_loading
  DEFAULT_WAREHOUSE = sustainability_loading_wh
  MUST_CHANGE_PASSWORD = TRUE; 
 
{%- endif %}

{% if hasDsSandboxDb -%}

CREATE USER IF NOT EXISTS bi_tool_ds_usr
  LOGIN_NAME   = 'bi_tool_ds_usr'
  DEFAULT_ROLE = bi_tool_ds
  DEFAULT_WAREHOUSE = bi_tool_ds_wh
  MUST_CHANGE_PASSWORD = TRUE; 
 
{%- endif %}

USE ROLE {{ defaultRole }};