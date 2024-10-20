-- ------------------------------------------------------------------------------------------------------------------------------
-- Create users
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;

CREATE USER IF NOT EXISTS sigma_{{ environment }}_usr
    LOGIN_NAME   = 'sigma_{{ environment }}_usr'
    DEFAULT_ROLE = bi_tool_{{ environment }}
    DEFAULT_WAREHOUSE = sigma_{{ environment }}_wh
    MUST_CHANGE_PASSWORD = TRUE; 

-- TODO: This should use a dedicated, environment specific, warehouse. To be fixed by AB#78885
CREATE USER IF NOT EXISTS app_dashboards_{{ environment }}_usr 
  DEFAULT_ROLE = app_{{ environment }}_f
  DEFAULT_WAREHOUSE = app_dashboards_{{ environment }}_wh
  MUST_CHANGE_PASSWORD = TRUE; 

CREATE USER IF NOT EXISTS data_pipeline_{{ environment }}_usr
    LOGIN_NAME   = 'data_pipeline_{{ environment }}_usr'
    DEFAULT_ROLE = data_pipeline_{{ environment }}
    DEFAULT_WAREHOUSE = data_pipeline_{{ environment }}_wh
    MUST_CHANGE_PASSWORD = TRUE; 

CREATE USER IF NOT EXISTS ml_pipeline_{{ environment }}_usr
    LOGIN_NAME   = 'ml_pipeline_{{ environment }}_usr'
    DEFAULT_ROLE = ml_pipeline_{{ environment }}
    DEFAULT_WAREHOUSE = ml_pipeline_{{ environment }}_wh
    MUST_CHANGE_PASSWORD = TRUE; 

USE ROLE {{ defaultRole }};
