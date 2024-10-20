-- ------------------------------------------------------------------------------------------------------------------------------
-- This table stores site/building level default values used for calculations
-- These values should be eventually only coming via ADT.
-- Combination of 'site_id' and 'type' should be unique.
-- Column 'default_value' can be used to store scalar default values such as 3.14159265359
-- But also to store additional additional context like so: { "value": 22.5, "unit": "degC" }
-- It is a good idea to do TRY_CAST when using these values like so: TRY_CAST(default_value:value::string AS DOUBLE)
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE transformed.site_defaults (
  site_id                VARCHAR(36) NOT NULL, 
  type                   VARCHAR(255) NOT NULL, 
  default_value          VARIANT NULL,
  _is_active             BOOLEAN DEFAULT TRUE NOT NULL,
  _valid_from            TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('2000-01-01') NOT NULL,
  _valid_to              TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('9999-12-31') NOT NULL,
  _created_at            TIMESTAMP_NTZ   NOT NULL    DEFAULT SYSDATE(),
  _created_by_task       VARCHAR(255)    NULL,
  _last_updated_at       TIMESTAMP_NTZ   NOT NULL    DEFAULT SYSDATE(),
  _last_updated_by_task  VARCHAR(255)    NULL
);