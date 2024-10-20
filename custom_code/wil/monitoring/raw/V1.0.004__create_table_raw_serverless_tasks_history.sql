-- ------------------------------------------------------------------------------------------------------------------------------
-- Create raw serverless tasks history table
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS  central_monitoring_db.raw.serverless_tasks_history (
  account_name      TEXT,
  database_name     TEXT,
  schema_name       TEXT,
  task_name         TEXT,
  task_id           NUMBER,
  start_time        TIMESTAMP_LTZ,
  end_time          TIMESTAMP_LTZ,
  credits_used      NUMBER(38,9),
  _exported_at      TIMESTAMP_LTZ,
  _loader_run_id    VARCHAR(36)
);