---------------------------------------------------------------------------------------
-- Create table for storing temperature measurements for comfort metrics calculation
-- ------------------------------------------------------------------------------------

CREATE TRANSIENT TABLE IF NOT EXISTS transformed.return_air_temperature_measurements (
  asset_twin_id         VARCHAR(255)        NOT NULL,
  site_id				VARCHAR(36) 		NULL,
  sensor_trend_id       VARCHAR(36)         NOT NULL,
  setpoint_trend_id     VARCHAR(36)         NULL,
  captured_at           TIMESTAMP_NTZ(9)    NOT NULL,
  return_air_temperature       DOUBLE       NULL,
  return_air_temperature_sp    DOUBLE       NULL,
  deviation             DOUBLE              NULL,
  return_air_humidity	DOUBLE				NULL,
  _created_at           TIMESTAMP_NTZ       NOT NULL    DEFAULT SYSDATE(),
  _created_by_task      VARCHAR(255)        NULL
);