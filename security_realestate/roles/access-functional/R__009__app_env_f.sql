-- ------------------------------------------------------------------------------------------------------------------------------
-- Grant environment specific access roles to 'app_<env>_f' functional role
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;
GRANT ROLE {{ environment }}_app_dashboards_r TO ROLE app_{{ environment }}_f;

USE ROLE {{ defaultRole }};