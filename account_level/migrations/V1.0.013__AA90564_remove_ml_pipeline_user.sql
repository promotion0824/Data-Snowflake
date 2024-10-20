-- ------------------------------------------------------------------------------------
-- Remove 'ml_pipeline' user, role and warehouse as it is getting replaced by 'ml_pipeline_<env>'
-- ------------------------------------------------------------------------------------
USE ROLE USERADMIN;

DROP ROLE IF EXISTS ml_pipeline;

USE ROLE {{ defaultRole }};

DROP WAREHOUSE IF EXISTS ml_pipeline_wh;

DROP USER IF EXISTS ml_pipeline_usr;

USE ROLE {{ defaultRole }};