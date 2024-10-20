-- ------------------------------------------------------------------------------------------------------------------------------
-- Initially populate table with existing data
-- This table is then appended into by a task that runs on schedule
-- ------------------------------------------------------------------------------------------------------------------------------

INSERT INTO transformed.site_daily_comfort_scores (
  site_id, 
  date, 
  measurements_count, 
  measurements_in_range_count, 
  score_value, 
  _created_at,
   _last_updated_at
) 
  WITH cte_temp_measurements AS (
    SELECT
        tsm.site_id,
        captured_at_local,
        CASE 
          WHEN tsm.measurement_value_degc >= t_low.threshold_value AND tsm.measurement_value_degc <= t_high.threshold_value THEN true
          ELSE false
        END AS is_in_acceptable_range
      FROM transformed.zone_air_temperature_sensor_measurements tsm
        JOIN utils.dates d 
          ON (TO_DATE(tsm.captured_at_local) = d.date AND d.is_weekday = true)  
        JOIN transformed.site_thresholds t_low 
          ON (tsm.site_id = t_low.site_id AND t_low.type = 'ZoneAirTemperatureDegC_Low' AND tsm.captured_at_local BETWEEN t_low._valid_from AND t_low._valid_to)
        JOIN transformed.site_thresholds t_high 
          ON (tsm.site_id = t_high.site_id AND t_high.type = 'ZoneAirTemperatureDegC_High' AND tsm.captured_at_local BETWEEN t_high._valid_from AND t_high._valid_to)  
      -- TODO: This should come from site configuration
      WHERE TO_TIME(tsm.captured_at_local) >= TO_TIME('08:00:00') AND TO_TIME(tsm.captured_at_local) <= TO_TIME('18:00:00')
  )

  SELECT 
    site_id, 
    TO_DATE(captured_at_local) AS date,
    COUNT(1) AS measurements_count,
    SUM(CASE WHEN is_in_acceptable_range THEN 1 ELSE 0 END) AS measurements_in_range_count,
    CAST(measurements_in_range_count / measurements_count AS NUMERIC(3,2)) AS score_value,
    SYSDATE() AS _created_at,
    SYSDATE() AS _last_updated_at
  FROM cte_temp_measurements
  GROUP BY 
    site_id, 
    date
;

