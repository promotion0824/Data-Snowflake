---------------------------------------------------------------------------------------
-- Create table for storing temperature measurements for comfort metrics calculation
-- ------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE transformed.chiller_delta_measurements (
  asset_id              VARCHAR(255),
  site_id               VARCHAR(36),
  trend_id              VARCHAR(36),
  captured_at           TIMESTAMP_NTZ,
  date_time_local_15min TIMESTAMP_NTZ,
  unit                  VARCHAR(36),
  sensor_type           VARCHAR(36),
  chiller_delta_temp    DOUBLE,
  chiller_run_status    BOOLEAN,
  _valid_from           TIMESTAMP_NTZ,
  _valid_to             TIMESTAMP_NTZ,
  _created_at           TIMESTAMP_NTZ  DEFAULT SYSDATE(),
  _created_by_task      VARCHAR(255),
  last_enqueued_at_utc  TIMESTAMP_NTZ
);
