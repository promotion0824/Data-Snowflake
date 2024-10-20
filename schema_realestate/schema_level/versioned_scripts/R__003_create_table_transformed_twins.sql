-- ------------------------------------------------------------------------------------------------------------------------------
-- Create table
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS transformed.twins (
      twin_id                         VARCHAR(255),
      name                            VARCHAR(255),
      model_id                        VARCHAR(255),
      trend_id                        VARCHAR(100),
      unique_id                       VARCHAR(100),
      external_id                     VARCHAR(255),
      floor_id                        VARCHAR(100),
      floor_dtid                      VARCHAR(100),      
      site_id                         VARCHAR(100),
      site_dtid                       VARCHAR(100),
      connector_id                    VARCHAR(100),
      geometry_viewer_id              VARCHAR(100),
      tags                            VARCHAR(1000),
      is_deleted                      BOOLEAN,
      export_time                     TIMESTAMP_NTZ,
      raw_json_value                  VARIANT,
       _is_active                     BOOLEAN,
      _created_at                     TIMESTAMP_NTZ,
      _last_updated_at                TIMESTAMP_NTZ,
      _stage_record_id                STRING,
      _loader_run_id                  VARCHAR(36),
      _ingested_at                    TIMESTAMP_NTZ,
      _staged_at                      TIMESTAMP_NTZ
); 
