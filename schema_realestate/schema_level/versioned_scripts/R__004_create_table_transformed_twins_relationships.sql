-- ------------------------------------------------------------------------------------------------------------------------------
-- Create table
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transformed.twins_relationships (
      source_twin_id                  VARCHAR(255),
      relationship_name               VARCHAR(255),
      target_twin_id                  VARCHAR(255),
      relationship_id                 VARCHAR(1000),
      is_deleted                      BOOLEAN,
      export_time                     TIMESTAMP_NTZ,
      raw_json_value                  VARIANT,
      _is_active                      BOOLEAN,
      _created_at                     TIMESTAMP_NTZ,
      _last_updated_at                TIMESTAMP_NTZ,
      _stage_record_id                STRING,
      _loader_run_id                  VARCHAR(36),
      _ingested_at                    TIMESTAMP_NTZ,
      _staged_at                      TIMESTAMP_NTZ
);