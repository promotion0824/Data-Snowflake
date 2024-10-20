-- ------------------------------------------------------------------------------------------------------------------------------
-- This table stores site/building level thresholds used for score calculations
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE transformed.site_thresholds (
  id                     INT IDENTITY(1,1) NOT NULL,
  site_id                VARCHAR(36) NOT NULL, 
  type                   VARCHAR(255) NOT NULL, 
  threshold_value        NUMERIC(18,6) NOT NULL,
  settings               VARIANT NULL,
  _is_active             BOOLEAN DEFAULT TRUE NOT NULL,
  _valid_from            TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('2000-01-01') NOT NULL,
  _valid_to              TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('9999-12-31') NOT NULL,
  _created_at            TIMESTAMP_NTZ DEFAULT SYSDATE() NOT NULL,
  _last_modified_at      TIMESTAMP_NTZ DEFAULT SYSDATE() NOT NULL
);