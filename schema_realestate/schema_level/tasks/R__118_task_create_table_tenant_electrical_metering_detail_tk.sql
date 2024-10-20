-- ------------------------------------------------------------------------------------------------------------------------------
-- Task to persist view as table in transformed
-- ------------------------------------------------------------------------------------------------------------------------------
ALTER TASK IF EXISTS transformed.merge_agg_electrical_metering_hourly_tk SUSPEND;


CREATE OR REPLACE TASK transformed.tenant_electrical_metering_detail_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  AFTER transformed.merge_agg_electrical_metering_daily_tk
AS
CREATE OR REPLACE TABLE transformed.tenant_electrical_metering_detail AS SELECT * FROM transformed.tenant_electrical_metering_detail_v;
  
ALTER TASK IF EXISTS transformed.tenant_electrical_metering_detail_tk RESUME;
ALTER TASK IF EXISTS transformed.merge_agg_electrical_metering_hourly_tk RESUME;