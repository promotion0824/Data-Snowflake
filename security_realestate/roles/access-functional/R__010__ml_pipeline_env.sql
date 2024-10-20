-- ------------------------------------------------------------------------------------------------------------------------------
-- Grant environment specific access roles to 'ml_pipeline_<env>' functional role
-- ------------------------------------------------------------------------------------------------------------------------------
USE ROLE USERADMIN;

GRANT ROLE {{ environment }}_raw_r TO ROLE ml_pipeline_{{ environment }};
GRANT ROLE {{ environment }}_transformed_r TO ROLE ml_pipeline_{{ environment }};
GRANT ROLE {{ environment }}_published_r TO ROLE ml_pipeline_{{ environment }};

-- Enable view creation in 'published' schema
GRANT ROLE {{ environment }}_published_ddl TO ROLE ml_pipeline_{{ environment }};

GRANT ROLE {{ environment }}_ml_w TO ROLE ml_pipeline_{{ environment }};
GRANT ROLE {{ environment }}_ml_ddl TO ROLE ml_pipeline_{{ environment }};

USE ROLE {{ defaultRole }};