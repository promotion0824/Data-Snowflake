-- ------------------------------------------------------------------------------------------------------------------------------
-- Create 'ml_pipeline_<env>' functional role
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS ml_pipeline_{{ environment }};

USE ROLE {{ defaultRole }};
GRANT USAGE ON WAREHOUSE ml_pipeline_{{ environment }}_wh TO ROLE ml_pipeline_{{ environment }};
GRANT OPERATE ON WAREHOUSE ml_pipeline_{{ environment }}_wh TO ROLE ml_pipeline_{{ environment }};
GRANT ROLE ml_pipeline_{{ environment }} TO ROLE SYSADMIN;