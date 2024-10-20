----------------------------------------------------------------------------------
-- Task that aggregates to the daily level
-- ------------------------------------------------------------------------------------------------------------------------------
ALTER TASK IF EXISTS transformed.merge_agg_electrical_metering_hourly_tk SUSPEND;

CREATE OR REPLACE TASK transformed.merge_agg_electrical_demand_hourly_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  AFTER transformed.merge_agg_electrical_metering_hourly_tk
AS
  CALL transformed.merge_agg_electrical_demand_hourly_sp(SYSTEM$CURRENT_USER_TASK_NAME())
;

ALTER TASK IF EXISTS transformed.merge_agg_electrical_demand_hourly_tk RESUME;
ALTER TASK IF EXISTS transformed.merge_agg_electrical_metering_hourly_tk RESUME;
