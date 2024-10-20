-- ------------------------------------------------------------------------------------------------------------------------------
-- Create transformed views
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW transformed.total_elec_energy_sensor_raw_measurements AS
  SELECT
    ts.site_id,
    ts.trend_id,
    ts.captured_at,
    CONVERT_TIMEZONE('UTC',  s.time_zone, ts.captured_at) AS captured_at_local,
    CASE es.unit 
      WHEN 'Wh' THEN ts.telemetry_value / 1000.0 
      WHEN 'kWh' THEN ts.telemetry_value
      ELSE NULL
    END AS measurement_value_kwh
  FROM raw.time_series ts
    JOIN transformed.sites s
      ON (ts.site_id = s.site_id)
    JOIN transformed.total_elec_energy_sensors es
      ON (ts.trend_id = es.trend_id);  

CREATE OR REPLACE VIEW transformed.daily_usage_per_total_elec_energy_sensor AS
  WITH cte_last_measurement_per_day AS (

    SELECT
        site_id,
        trend_id,
        captured_at_local,
        TO_DATE(captured_at_local) AS date,
        measurement_value_kwh AS measurement_value_kwh,
        ROW_NUMBER() OVER (PARTITION BY trend_id, date ORDER BY captured_at_local DESC) AS row_number
    FROM transformed.total_elec_energy_sensor_raw_measurements
    QUALIFY row_number = 1

  )

  SELECT 
    site_id,
    trend_id,
    date,
    measurement_value_kwh AS last_daily_measurement_value_kwh,
    measurement_value_kwh - LAG(measurement_value_kwh, 1) OVER (PARTITION BY trend_id ORDER BY date) AS daily_usage_kwh
  FROM cte_last_measurement_per_day
; 

CREATE OR REPLACE VIEW transformed.zone_air_temperature_sensors AS
  SELECT
    unique_id,
    twin_id AS id,
    trend_id,
    site_id,
    external_id,
    name,
    raw_json_value:customProperties.description::VARCHAR(100)AS description,    
    raw_json_value:customProperties.type::VARCHAR(100) AS type,
    raw_json_value:customProperties.unit::VARCHAR(100) AS unit,
    raw_json_value:customProperties.trendInterval::NUMBER(38,0) AS trend_interval,
    raw_json_value:customProperties.enabled::VARCHAR(100) AS is_enabled,
    tags,
    raw_json_value,
    _is_active,
    _stage_record_id,
    _loader_run_id,
    _ingested_at,
    _staged_at
  FROM transformed.twins
  WHERE model_id = 'dtmi:com:willowinc:ZoneAirTemperatureSensor;1';

CREATE OR REPLACE VIEW transformed.zone_air_temperature_sensor_measurements AS
  SELECT
    ts.site_id,
    ts.trend_id,
    ts.captured_at,
    CONVERT_TIMEZONE('UTC',  s.time_zone, ts.captured_at) AS captured_at_local,
    telemetry_value AS measurement_value_degc
  FROM raw.time_series ts
    JOIN transformed.sites s
      ON (ts.site_id = s.site_id)
    JOIN transformed.zone_air_temperature_sensors sensors
      ON (ts.trend_id = sensors.trend_id)  
;    

CREATE OR REPLACE VIEW transformed.site_daily_energy_scores AS (
  SELECT 
    actual.site_id,
    actual.date,
    threshold.is_weekday,
    CASE 
      WHEN actual.daily_usage_kwh <= threshold.daily_low_threshold_kwh THEN 1
      WHEN actual.daily_usage_kwh >= threshold.daily_high_threshold_kwh THEN 0
      ELSE (threshold.daily_high_threshold_kwh - actual.daily_usage_kwh) / (threshold.daily_high_threshold_kwh - threshold.daily_low_threshold_kwh)
    END AS daily_energy_score,  
    actual.last_daily_measurement_value_kwh,
    actual.daily_usage_kwh,
    threshold.daily_low_threshold_kwh,
    threshold.daily_high_threshold_kwh
  FROM transformed.site_daily_electrical_energy_usage actual
    LEFT JOIN transformed.site_daily_energy_thresholds threshold 
      ON (actual.site_id = threshold.site_id AND actual.date = threshold.date)
);

CREATE OR REPLACE VIEW transformed.portfolio_operations_metrics
AS
  SELECT 
    dcs.portfolio_id,
    dcs.site_id,
    dcs.name AS site_name,
    d.date,
    d.is_weekday,
    deu.last_daily_measurement_value_kwh AS energy_last_daily_measurement_value_kwh,
    deu.daily_usage_kwh AS energy_daily_usage_kwh,
    det.daily_low_threshold_kwh AS energy_daily_low_threshold_kwh,
    det.daily_high_threshold_kwh AS energy_daily_high_threshold_kwh,
    det.monthly_low_threshold_cum_sum_kwh AS energy_monthly_low_threshold_cum_sum_kwh,
    det.monthly_high_threshold_cum_sum_kwh AS energy_monthly_high_threshold_cum_sum_kwh,
    det.total_low_threshold_cum_sum_kwh AS energy_overall_low_threshold_cum_sum_kwh,
    det.total_high_threshold_cum_sum_kwh AS energy_overall_high_threshold_cum_sum_kwh,
    ZEROIFNULL(scs.measurements_count) AS comfort_measurements_count,
    ZEROIFNULL(scs.measurements_in_range_count) AS comfort_measurements_in_range_count
  FROM transformed.directory_core_sites dcs
    CROSS JOIN utils.dates d
    LEFT JOIN transformed.site_daily_electrical_energy_usage deu
      ON (dcs.site_id = deu.site_id AND d.date = deu.date)
    LEFT JOIN transformed.site_daily_energy_thresholds det
      ON (dcs.site_id = det.site_id AND d.date = det.date)
    LEFT JOIN transformed.site_daily_comfort_scores scs
      ON (dcs.site_id = scs.site_id AND d.date = scs.date)
  -- Filter only to latest year of data
  WHERE 
    d.date BETWEEN TO_DATE(DATEADD(YEAR, -1, CONVERT_TIMEZONE('Australia/Sydney', CURRENT_TIMESTAMP()))) 
      AND TO_DATE(DATEADD(DAY, -1, CONVERT_TIMEZONE('Australia/Sydney', CURRENT_TIMESTAMP())))
;