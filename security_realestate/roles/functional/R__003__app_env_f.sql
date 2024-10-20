-- ------------------------------------------------------------------------------------------------------------------------------
-- Create 'app_<env>_f' functional role
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;
-- Ideally this role's name would be without the '_f' but we need to keep this backward compatible as the app API uses the role.
CREATE ROLE IF NOT EXISTS app_{{ environment }}_f;

USE ROLE {{ defaultRole }};

-- TODO: This should use a dedicated, environment specific, warehouse. To be fixed by AB#78885
GRANT USAGE ON WAREHOUSE {{ environment }}_wh TO ROLE app_{{ environment }}_f;
GRANT OPERATE ON WAREHOUSE {{ environment }}_wh TO ROLE app_{{ environment }}_f;

GRANT USAGE ON WAREHOUSE app_dashboards_{{ environment }}_wh TO ROLE app_{{ environment }}_f;
GRANT OPERATE ON WAREHOUSE app_dashboards_{{ environment }}_wh TO ROLE app_{{ environment }}_f;

GRANT ROLE app_{{ environment }}_f TO ROLE SYSADMIN;