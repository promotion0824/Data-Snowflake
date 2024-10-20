---------------------------------------------------------------------------------------
-- Create table for storing pre-aggregated data
-- ------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE transformed.chiller_efficiency_15min (
  asset_id                    VARCHAR(255),
  site_id					            VARCHAR(36),
  date_local                  DATE,
  date_time_local_15min       TIMESTAMP_NTZ(9),
  unit                        VARCHAR(36),
  sensor_type                 VARCHAR(255),
  avg_sensor_value            DOUBLE,
  count_run_sensor_on         INT,
  count_run_sensor_off        INT,
  _created_at                 TIMESTAMP_NTZ       DEFAULT SYSDATE(),
  _created_by_task            VARCHAR(255),
  _last_updated_at            TIMESTAMP_NTZ       DEFAULT SYSDATE(),
  _last_updated_by_task       VARCHAR(255)

);