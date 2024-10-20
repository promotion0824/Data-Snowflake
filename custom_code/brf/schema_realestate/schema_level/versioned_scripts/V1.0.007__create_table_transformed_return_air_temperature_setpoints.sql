----------------------------------------------------------------------------------
-- Create table for storing setpoints for comfort metrics calculation
-- -------------------------------------------------------------------------------

CREATE TRANSIENT TABLE transformed.return_air_temperature_setpoints (
  asset_twin_id         VARCHAR(255)        NOT NULL,
  trend_id              VARCHAR(36)         NOT NULL,
  site_id				VARCHAR(36)         NULL,
  first_captured_at     TIMESTAMP_NTZ(9)    NOT NULL,
  return_air_temperature_sp    DOUBLE       NOT NULL,
  _valid_from           TIMESTAMP_NTZ       NOT NULL    DEFAULT TO_TIMESTAMP_NTZ('0000-01-01 00:00:00.000'),
  _valid_to             TIMESTAMP_NTZ       NOT NULL    DEFAULT TO_TIMESTAMP_NTZ('9999-12-31 23:59:59.999'),
  _created_at           TIMESTAMP_NTZ       NOT NULL    DEFAULT SYSDATE(),
  _created_by_task      VARCHAR(255)        NULL,
  _last_updated_at      TIMESTAMP_NTZ       NOT NULL    DEFAULT SYSDATE(),
  _last_updated_by_task VARCHAR(255)        NULL
);
