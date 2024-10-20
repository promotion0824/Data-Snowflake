-- ------------------------------------------------------------------------------------------------------------------------------
-- Grant roles to users
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;

GRANT ROLE bi_tool_{{ environment }} TO USER sigma_{{ environment }}_usr;

GRANT ROLE app_{{ environment }}_f TO USER app_dashboards_{{ environment }}_usr;

GRANT ROLE data_pipeline_{{ environment }} TO USER data_pipeline_{{ environment }}_usr;
GRANT ROLE ml_pipeline_{{ environment }} TO USER ml_pipeline_{{ environment }}_usr;

USE ROLE {{ defaultRole }};