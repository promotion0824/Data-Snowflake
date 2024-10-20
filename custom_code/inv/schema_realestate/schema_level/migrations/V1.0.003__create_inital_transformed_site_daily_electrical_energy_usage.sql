-- ------------------------------------------------------------------------------------------------------------------------------
-- Initially populate table with existing data
-- This table is then appended into by a task that runs on schedule
-- ------------------------------------------------------------------------------------------------------------------------------

INSERT INTO transformed.site_daily_electrical_energy_usage (
  site_id, 
  date, 
  last_daily_measurement_value_kwh, 
  daily_usage_kwh, 
  _created_at,
  _last_updated_at
) 
  SELECT 
    site_id,
    date,
    SUM(last_daily_measurement_value_kwh) AS last_daily_measurement_value_kwh,
    SUM(daily_usage_kwh) AS daily_usage_kwh,
    SYSDATE() AS _created_at,
    SYSDATE() AS _last_updated_at
  FROM transformed.daily_usage_per_total_elec_energy_sensor
  GROUP BY 
    site_id,
    date
;
