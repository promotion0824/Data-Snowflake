-- ******************************************************************************************************************************
-- Task to trigger stored procedure
-- ******************************************************************************************************************************
ALTER TASK IF EXISTS transformed.merge_sites_stations_tk SUSPEND;

CREATE OR REPLACE TASK transformed.merge_weather_data_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  AFTER transformed.merge_sites_stations_tk
AS
  CALL transformed.merge_weather_data_sp();

ALTER TASK IF EXISTS transformed.merge_weather_data_tk SUSPEND;
ALTER TASK IF EXISTS transformed.merge_sites_stations_tk SUSPEND;