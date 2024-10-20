-- ------------------------------------------------------------------------------------------------------------------------------
-- Legacy roles and users
-- These roles and users will be eventually replaced and decommissioned
-- ------------------------------------------------------------------------------------------------------------------------------
-- This is used:

-- for weather data elt from wo77920 to all other accounts
-- TODO: 
-- This needs to be fixed. This is an elevated permissions role 
-- that should not be used for data pipelines
USE ROLE USERADMIN;

GRANT ROLE data_pipeline_{{ environment }} TO USER DATAENGINEERINGSERVICE;

USE ROLE {{ defaultRole }};