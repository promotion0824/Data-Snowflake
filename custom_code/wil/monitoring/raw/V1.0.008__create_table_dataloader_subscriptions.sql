-- ------------------------------------------------------------------------------------------------------------------------------
-- Create raw account details table
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS  central_monitoring_db.transformed.dataloader_subscriptions (
  source_type			TEXT,
  source_unique_name    TEXT,
  source_is_active      BOOLEAN,
  region     			TEXT,
  entity_uniquename     TEXT,
  entity_is_active		BOOLEAN,
  trigger_name			TEXT,
  dataset_settings		VARIANT,
  sink_settings			VARIANT,
  _ingested_at          TIMESTAMP_LTZ,
  _loader_run_id        VARCHAR(36)
);
