CREATE OR REPLACE VIEW published.portfolio_operations_metrics
AS
  SELECT 
    portfolio_id,
    site_id,
    site_name,
    date,
    is_weekday,
    energy_last_daily_measurement_value_kwh,
    energy_daily_usage_kwh,
    energy_daily_low_threshold_kwh,
    energy_daily_high_threshold_kwh,
    energy_monthly_low_threshold_cum_sum_kwh,
    energy_monthly_high_threshold_cum_sum_kwh,
    energy_overall_low_threshold_cum_sum_kwh,
    energy_overall_high_threshold_cum_sum_kwh,
    comfort_measurements_count,
    comfort_measurements_in_range_count
  FROM transformed.portfolio_operations_metrics;

CREATE OR REPLACE VIEW published.site_thresholds
AS
  SELECT 
    id,
    site_id,
    type,
    threshold_value,
    settings
  FROM transformed.site_thresholds
  WHERE _is_active = true;

CREATE OR REPLACE VIEW published.directory_core_sites
AS
  SELECT 
    site_id,
    portfolio_id,
    customer_id,
    name,
    time_zone
  FROM transformed.directory_core_sites
  WHERE _is_active = true;

CREATE OR REPLACE VIEW published.zone_air_temperature_sensors
AS
  SELECT 
    unique_id,
    id,
    trend_id,
    site_id,
    external_id,
    name,
    description,
    type,
    unit,
    trend_interval,
    is_enabled,
    tags,
    raw_json_value
  FROM transformed.zone_air_temperature_sensors
  WHERE _is_active = true;

CREATE OR REPLACE VIEW published.zone_air_temperature_sensor_measurements
AS
  SELECT 
    site_id,
    trend_id,
    captured_at,
    captured_at_local,
    measurement_value_degc
  FROM transformed.zone_air_temperature_sensor_measurements;

CREATE OR REPLACE VIEW published.site_daily_comfort_scores
AS
  SELECT 
    site_id,
    date,
    measurements_count,
    measurements_in_range_count,
    score_value
  FROM transformed.site_daily_comfort_scores;

CREATE OR REPLACE VIEW published.site_daily_electrical_energy_usage
AS
  SELECT 
    site_id,
    date,
    last_daily_measurement_value_kwh,
    daily_usage_kwh
  FROM transformed.site_daily_electrical_energy_usage;
