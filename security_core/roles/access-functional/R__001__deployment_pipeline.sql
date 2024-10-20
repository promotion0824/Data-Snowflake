-- ------------------------------------------------------------------------------------------------------------------------------
-- Grant access roles to 'deployment_pipeline' functional role
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE SYSADMIN;

GRANT ROLE util_public_ddl TO ROLE deployment_pipeline;
GRANT ROLE util_schemachange_ddl TO ROLE deployment_pipeline;

USE ROLE {{ defaultRole }};