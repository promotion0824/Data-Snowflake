-- ------------------------------------------------------------------------------------------------------------------------------
-- Create role for loading Segment events
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS segment_loading;
-- Grant segment_loading role access to the virtual warehouse:
GRANT USAGE ON WAREHOUSE segment_loading_wh TO ROLE segment_loading;

-- Grant segment_loading role access to segment_events_db database
GRANT USAGE ON DATABASE segment_events_db TO ROLE segment_loading;

-- Grant segment_loading role create schema permissions in segment_events_db database
GRANT CREATE SCHEMA ON DATABASE segment_events_db TO ROLE segment_loading;

-------------------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS segment_reporting;
-- Grant segment_reporting role access to the virtual warehouse:
GRANT USAGE ON WAREHOUSE segment_reporting_wh TO ROLE segment_reporting;

-- Grant segment_reporting role access to segment_events_db database
GRANT USAGE ON DATABASE segment_events_db TO ROLE segment_reporting;
GRANT USAGE ON SCHEMA segment_events_db.published TO ROLE segment_reporting;
GRANT SELECT ON ALL VIEWS IN SCHEMA segment_events_db.published TO ROLE segment_reporting;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA segment_events_db.published TO ROLE segment_reporting;
-------------------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS weather_loading;
GRANT USAGE ON WAREHOUSE weather_loading_wh TO ROLE weather_loading;

-- Grant segment_reporting role access to segment_events_db database
GRANT USAGE ON DATABASE {{ environment }}_db TO ROLE weather_loading;
GRANT USAGE ON SCHEMA {{ environment }}_db.published TO ROLE weather_loading;
GRANT SELECT ON ALL VIEWS IN SCHEMA {{ environment }}_db.published TO ROLE weather_loading;

GRANT SELECT ON FUTURE VIEWS IN SCHEMA {{ environment }}_db.published TO ROLE weather_loading;

USE ROLE ACCOUNTADMIN;
GRANT ROLE WEATHER_LOADING TO ROLE SYSADMIN;

GRANT ROLE {{ environment }}_published_r TO ROLE data_pipeline_{{ environment }};

USE ROLE {{ defaultRole }};