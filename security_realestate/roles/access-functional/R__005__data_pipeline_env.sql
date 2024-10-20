-- ------------------------------------------------------------------------------------------------------------------------------
-- Grant environment specific access roles to 'data_pipeline_<env>' functional role
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;

GRANT ROLE {{ environment }}_raw_r TO ROLE data_pipeline_{{ environment }};
GRANT ROLE {{ environment }}_raw_w TO ROLE data_pipeline_{{ environment }};

GRANT ROLE {{ environment }}_transformed_r TO ROLE data_pipeline_{{ environment }};
GRANT ROLE {{ environment }}_transformed_w TO ROLE data_pipeline_{{ environment }};

-- The data_pipeline role needs DDL access to raw schema because it creates a transient stage
-- in the schema in order to copy data from one SF instance to another using ADF. If it doesn't have the permission,
-- It fails with SQL access control error: Insufficient privileges to operate on schema 'RAW'
-- This assignment is probably just temporary as we will be replacing these Snowflake to Snowflake ADF pipelines.
GRANT ROLE {{ environment }}_raw_ddl TO ROLE data_pipeline_{{ environment }};

USE ROLE {{ defaultRole }};