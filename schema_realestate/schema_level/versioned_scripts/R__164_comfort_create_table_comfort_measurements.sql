---------------------------------------------------------------------------------------
-- Create table for storing temperature measurements for comfort metrics calculation
-- ------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS transformed.comfort_measurements (
  asset_id              VARCHAR(255),
  sensor_trend_id       VARCHAR(36),
  captured_at           TIMESTAMP_NTZ,
  unit                  VARCHAR(36),
  setpoint_type         VARCHAR(36),
  zone_air_temp         DOUBLE,
  min_setpoint_value    DOUBLE,
  max_setpoint_value    DOUBLE,
  _created_at           TIMESTAMP_NTZ  DEFAULT SYSDATE(),
  _created_by_task      VARCHAR(255),
  last_enqueued_at      TIMESTAMP_NTZ
);
