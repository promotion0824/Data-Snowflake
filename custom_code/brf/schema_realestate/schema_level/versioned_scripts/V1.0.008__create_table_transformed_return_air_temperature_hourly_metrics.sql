---------------------------------------------------------------------------------------
-- Create table for storing aggregated hourly comfort metrics
-- ------------------------------------------------------------------------------------

CREATE TRANSIENT TABLE IF NOT EXISTS transformed.return_air_temperature_hourly_metrics (
  asset_twin_id               VARCHAR(255)        NOT NULL,
  floor_id					  VARCHAR(36) 		  NULL,
  site_id					  VARCHAR(36) 		  NULL,
  date                        DATE                NOT NULL,
  date_hour_start             TIMESTAMP_NTZ(9)    NOT NULL,
  last_captured_at_local      TIMESTAMP_NTZ(9)    NOT NULL,
  last_captured_at_utc        TIMESTAMP_NTZ(9)    NOT NULL,
  is_working_hour             BOOLEAN             NOT NULL,
  day_of_week_type            VARCHAR(7)          NOT NULL,
  avg_return_air_temperature    DOUBLE       	  NULL,
  avg_return_air_temperature_sp DOUBLE       	  NULL,
  deviation      			  DOUBLE       		  NULL,
  avg_return_air_humidity	  DOUBLE			  NULL,
  sample_count                INT                 NOT NULL,
  _created_at                 TIMESTAMP_NTZ       NOT NULL    DEFAULT SYSDATE(),
  _created_by_task            VARCHAR(255)        NULL,
  _last_updated_at            TIMESTAMP_NTZ       NOT NULL    DEFAULT SYSDATE(),
  _last_updated_by_task       VARCHAR(255)        NULL
); 