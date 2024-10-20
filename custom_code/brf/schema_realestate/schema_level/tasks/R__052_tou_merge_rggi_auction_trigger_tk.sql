-- ------------------------------------------------------------------------------------------------------------------------------
-- Tasks that schedule stored procedure execution
--  12 minutes after each hour
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TASK raw.tou_merge_rggi_auction_trigger_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
  SCHEDULE = 'USING CRON 12 * * * * America/New_York'
AS
  CALL transformed.tou_merge_rggi_auction_sp()
;      

ALTER TASK raw.tou_merge_rggi_auction_trigger_tk RESUME;
