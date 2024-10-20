-- ------------------------------------------------------------------------------------------------------------------------------
-- Grant environment specific access roles to 'bi_tool_<env>' functional role
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;
GRANT ROLE {{ environment }}_published_r TO ROLE bi_tool_{{ environment }};

-- Permissions on analytics_db
GRANT ROLE analytics_public_r TO ROLE bi_tool_{{ environment }};
GRANT ROLE analytics_public_w TO ROLE bi_tool_{{ environment }};
GRANT ROLE analytics_public_ddl TO ROLE bi_tool_{{ environment }};

USE ROLE {{ defaultRole }};