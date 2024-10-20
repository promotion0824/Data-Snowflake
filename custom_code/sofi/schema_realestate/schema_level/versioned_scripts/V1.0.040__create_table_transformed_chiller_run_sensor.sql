----------------------------------------------------------------------------------
-- Create table for storing run_sensor for comfort metrics calculation
-- -------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE transformed.chiller_run_sensor (
  asset_id         VARCHAR(255)        ,
  trend_id              VARCHAR(36)         ,
  site_id               VARCHAR(36)         ,
  first_captured_at     TIMESTAMP_NTZ(9)    ,
  run_sensor_model_id   VARCHAR(255)        ,
  run_sensor_unit       VARCHAR(255)        ,
  run_sensor_value      DOUBLE              ,
  _valid_from           TIMESTAMP_NTZ       DEFAULT TO_TIMESTAMP_NTZ('0000-01-01 00:00:00.000'),
  _valid_to             TIMESTAMP_NTZ       DEFAULT TO_TIMESTAMP_NTZ('9999-12-31 23:59:59.999'),
  _created_at           TIMESTAMP_NTZ       DEFAULT SYSDATE(),
  _created_by_task      VARCHAR(255)        ,
  _last_updated_at      TIMESTAMP_NTZ       DEFAULT SYSDATE(),
  _last_updated_by_task VARCHAR(255)        
);
