-- ------------------------------------------------------------------------------------------------------------------------------
-- Create raw pipe status history table
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS  central_monitoring_db.raw.pipes_status_history (
  account_name      TEXT,
  name              TEXT,
  database_name     TEXT,
  schema_name       TEXT,
  _captured_at      TIMESTAMP_LTZ,
  pipe_status	      VARIANT
);
