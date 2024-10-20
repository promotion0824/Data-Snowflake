-- ------------------------------------------------------------------------------------------------------------------------------
-- Create global owner access role
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS global_owner;

GRANT ROLE global_owner TO ROLE deployment_pipeline;
GRANT ROLE global_owner TO ROLE SYSADMIN;
USE ROLE {{ defaultRole }};