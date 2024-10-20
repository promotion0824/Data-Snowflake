-- ------------------------------------------------------------------------------------------------------------------------------
-- Task to persist view as table in transformed
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TASK transformed.tenant_energy_utility_account_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  -- Once per day 15 minutes after 1am
  SCHEDULE = 'USING CRON 5 1 * * * UTC'
AS
CALL transformed.merge_tenant_energy_utility_account_sp() ;
  
ALTER TASK IF EXISTS transformed.tenant_energy_utility_account_tk RESUME;
