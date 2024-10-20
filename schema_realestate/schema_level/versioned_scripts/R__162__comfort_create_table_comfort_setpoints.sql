----------------------------------------------------------------------------------
-- Create table for storing setpoints for comfort metrics calculation
-- -------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS transformed.comfort_setpoints (
  asset_id              VARCHAR(255)    ,
  trend_id              VARCHAR(36)     ,
  external_id           VARCHAR(255)    ,
  first_captured_at     TIMESTAMP_NTZ(9),
  setpoint_model_id     VARCHAR(255)    ,
  setpoint_unit         VARCHAR(255)    ,
  setpoint_value        DOUBLE          ,
  _valid_from           TIMESTAMP_NTZ   DEFAULT TO_TIMESTAMP_NTZ('0000-01-01 00:00:00.000'),
  _valid_to             TIMESTAMP_NTZ   DEFAULT TO_TIMESTAMP_NTZ('9999-12-31 23:59:59.999'),
  _created_at           TIMESTAMP_NTZ   DEFAULT SYSDATE(),
  _created_by_task      VARCHAR(255)    ,
  _last_updated_at      TIMESTAMP_NTZ   DEFAULT SYSDATE(),
  _last_updated_by_task VARCHAR(255)        
);