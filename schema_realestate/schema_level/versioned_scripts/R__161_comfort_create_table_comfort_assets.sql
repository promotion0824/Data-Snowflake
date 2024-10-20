----------------------------------------------------------------------------------
-- Create table for storing assets for comfort metrics calculation
-- -------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS transformed.comfort_assets (
  model_id_asset        VARCHAR(255) ,
  asset_id              VARCHAR(255) ,
  asset_name            VARCHAR(1000),
  excludedFromComfortAnalytics BOOLEAN,
  sensor_trend_id       VARCHAR(36)  ,
  setpoint_trend_id     VARCHAR(36)  ,
  unit                  CHAR(4)      ,
  setpoint_model_id     VARCHAR(255) ,
  building_id           VARCHAR(255) ,
  building_name         VARCHAR(255) ,
  site_id               VARCHAR(36)  ,
  time_zone             VARCHAR(255) ,
  zone_id               VARCHAR(255) ,
  zone_name             VARCHAR(255) ,
  room_id               VARCHAR(255) ,
  room_name             VARCHAR(255) ,
  level_name            VARCHAR(255) ,
  floor_sort_order      INT          ,
  _last_updated_at      TIMESTAMP_NTZ    DEFAULT SYSDATE()
); 