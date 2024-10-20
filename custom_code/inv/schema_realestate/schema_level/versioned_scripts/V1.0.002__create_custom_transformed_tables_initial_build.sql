-- ******************************************************************************************************************************
-- Transformed tables
-- ******************************************************************************************************************************

-- ------------------------------------------------------------------------------------------------------------------------------
-- Total electrical energy sensors twins table
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE transformed.total_elec_energy_sensors (
  unique_id VARCHAR(36) NOT NULL,
  id VARCHAR(255) NOT NULL,
  trend_id VARCHAR(36) NOT NULL,
  site_id VARCHAR(36) NOT NULL,
  external_id VARCHAR(255) NOT NULL,
  name VARCHAR(255) NULL,
  description VARCHAR(16777216) NULL,
  type VARCHAR(255) NULL,
  unit VARCHAR(255) NULL,
  trend_interval INT NULL,
  is_enabled BOOLEAN NULL,
  tags VARIANT NULL,
  raw_json_value VARIANT NOT NULL,
  _is_active BOOLEAN NOT NULL DEFAULT true,
  _created_at TIMESTAMP_NTZ DEFAULT SYSDATE(),
  _last_updated_at TIMESTAMP_NTZ DEFAULT SYSDATE(),
  _stage_record_id STRING NOT NULL,
  _loader_run_id VARCHAR(36) NOT NULL,
  _ingested_at TIMESTAMP_NTZ NOT NULL,
  _staged_at TIMESTAMP_NTZ NOT NULL
);  

-- ------------------------------------------------------------------------------------------------------------------------------
-- Table that holds materialized site daily electrical energy usage
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE transformed.site_daily_electrical_energy_usage (
  site_id VARCHAR(36) NOT NULL,
  date DATE NOT NULL,
  last_daily_measurement_value_kwh NUMBER(18,6) NULL,
  daily_usage_kwh NUMBER(18,6) NULL,
  _created_at TIMESTAMP_NTZ DEFAULT SYSDATE(),
  _created_by_task VARCHAR(255) NULL,
  _last_updated_at TIMESTAMP_NTZ DEFAULT SYSDATE(),
  _last_updated_by_task VARCHAR(255) NULL
);  

-- ------------------------------------------------------------------------------------------------------------------------------
-- Table that holds materialized site daily energy thresholds and running totals
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE transformed.site_daily_energy_thresholds (
  site_id                               VARCHAR(36) NOT NULL,
  date                                  DATE NOT NULL,
  is_weekday                            BOOLEAN,
  initial_value_kwh                     NUMBER(18,6) NULL,
  daily_low_threshold_kwh               NUMBER(18,6) NULL,
  daily_high_threshold_kwh              NUMBER(18,6) NULL,
  monthly_low_threshold_cum_sum_kwh     NUMBER(18,6) NULL,
  monthly_high_threshold_cum_sum_kwh    NUMBER(18,6) NULL,
  total_low_threshold_cum_sum_kwh       NUMBER(18,6) NULL,
  total_high_threshold_cum_sum_kwh      NUMBER(18,6) NULL,
  _created_at                           TIMESTAMP_NTZ DEFAULT SYSDATE(),
  _created_by_task                      VARCHAR(255) NULL,
  _last_updated_at                      TIMESTAMP_NTZ DEFAULT SYSDATE(),
  _last_updated_by_task                 VARCHAR(255) NULL
); 

-- ------------------------------------------------------------------------------------------------------------------------------
-- Table that holds materialized site daily comfort scores
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE transformed.site_daily_comfort_scores (
  site_id VARCHAR(36) NOT NULL,
  date DATE NOT NULL,
  measurements_count INT NULL,
  measurements_in_range_count INT NULL,
  score_value NUMBER(3, 2) NULL,
  _created_at TIMESTAMP_NTZ DEFAULT SYSDATE(),
  _created_by_task VARCHAR(255) NULL,
  _last_updated_at TIMESTAMP_NTZ DEFAULT SYSDATE(),
  _last_updated_by_task VARCHAR(255) NULL
);  
