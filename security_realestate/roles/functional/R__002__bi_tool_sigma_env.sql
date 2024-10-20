-- ------------------------------------------------------------------------------------------------------------------------------
-- Create 'bi_tool_<env>' functional role
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS bi_tool_{{ environment }};

USE ROLE {{ defaultRole }};
GRANT USAGE ON WAREHOUSE sigma_{{ environment }}_wh TO ROLE bi_tool_{{ environment }};
GRANT OPERATE ON WAREHOUSE sigma_{{ environment }}_wh TO ROLE bi_tool_{{ environment }};
GRANT ROLE bi_tool_{{ environment }} TO ROLE SYSADMIN;
