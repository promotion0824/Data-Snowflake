---------------------------------------------------------------------------------------
-- Create table for storing aggregated hourly comfort metrics
-- ------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS transformed.comfort_hourly_metrics (
  asset_id                    VARCHAR(255),
  date                        DATE        ,
  date_hour_start             TIMESTAMP_NTZ(9),
  unit                        VARCHAR(36),
  avg_zone_air_temp           DOUBLE,
  min_setpoint_used           DOUBLE,
  max_setpoint_used           DOUBLE,
  setpoint_type               VARCHAR(100),
  offset_type_used            VARCHAR(100),
  heating_offset_used         DOUBLE,
  cooling_offset_used         DOUBLE,
  sample_count                INT,
  count_optimum               INT,
  comfort_score               NUMERIC(5, 2),
  is_working_hour             BOOLEAN,
  day_of_week_type            VARCHAR(7),
  last_captured_at_local      TIMESTAMP_NTZ(9),
  last_enqueued_at_utc        TIMESTAMP_NTZ(9),
  last_captured_at_utc        TIMESTAMP_NTZ(9),
  _created_at                 TIMESTAMP_NTZ       DEFAULT SYSDATE(),
  _created_by_task            VARCHAR(255),
  _last_updated_at            TIMESTAMP_NTZ       DEFAULT SYSDATE(),
  _last_updated_by_task       VARCHAR(255)

);