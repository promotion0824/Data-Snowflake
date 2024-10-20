-- ------------------------------------------------------------------------------------------------------------------------------
-- Create raw account details table
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS  central_monitoring_db.raw.account_details (
  account_name            TEXT,
  region                  TEXT,
  customer_identifier     TEXT,
  snowflake_version       TEXT,
  deployment_details      VARIANT,
  _captured_at            TIMESTAMP_LTZ,
  _loader_run_id          VARCHAR(36)
);