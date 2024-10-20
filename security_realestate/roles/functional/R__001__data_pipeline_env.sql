-- ------------------------------------------------------------------------------------------------------------------------------
-- Create 'data_pipeline_<env>' functional role
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS data_pipeline_{{ environment }};

USE ROLE {{ defaultRole }};
GRANT USAGE ON WAREHOUSE data_pipeline_{{ environment }}_wh TO ROLE data_pipeline_{{ environment }};
GRANT OPERATE ON WAREHOUSE data_pipeline_{{ environment }}_wh TO ROLE data_pipeline_{{ environment }};
GRANT ROLE data_pipeline_{{ environment }} TO ROLE SYSADMIN;