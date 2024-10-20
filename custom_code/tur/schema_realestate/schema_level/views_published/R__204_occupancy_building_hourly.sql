-- ------------------------------------------------------------------------------------------------------------------------------
-- create View
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW published.occupancy_building_hourly AS
WITH cte_hourly AS (
SELECT DISTINCT
    t.building_id, 
    t.model_id, 
    t.trend_id,
    ts.date_local,
    DATE_TRUNC('HOUR',ts.timestamp_local) AS date_local_hour,
    LAST_VALUE(ts.telemetry_value ) OVER (PARTITION BY t.building_id, t.model_id, t.trend_id, date_local_hour ORDER BY ts.timestamp_local) AS end_of_hour_value,
    MAX(ts.timestamp_local) OVER () AS last_refreshed_at_local
FROM transformed.occupancy_building_twins t
JOIN transformed.telemetry ts ON t.trend_id = ts.trend_id
WHERE ts.date_local >= '2024-02-23'  -- this is the first date with data;
)
SELECT
    ts.building_id,
    t.building_name,
    ts.model_id,
    t.capability_id,
    t.capability_name,
    ts.trend_id,
    ts.date_local,
    ts.date_local_hour,
    ts.end_of_hour_value,
    LAG(ts.end_of_hour_value, 1, 0) OVER (PARTITION BY ts.building_id, ts.model_id, t.trend_id ORDER BY ts.date_local_hour) AS previous_hour_value,
    GREATEST(0, end_of_hour_value - previous_hour_value)  AS hourly_incremental,
	dh.is_weekday,
    IFF(HOUR(dh.date_time_hour) >= COALESCE(working_hours.default_value:hourStart,8) 
        AND HOUR(dh.date_time_hour) <  COALESCE(working_hours.default_value:hourEnd, 18), true, false) AS is_working_hour,
    last_refreshed_at_local
FROM cte_hourly ts
JOIN transformed.occupancy_building_twins t ON ts.trend_id = t.trend_id
JOIN transformed.date_hour dh ON ts.date_local_hour = dh.date_time_hour
LEFT JOIN transformed.site_defaults working_hours
  ON (t.site_id = working_hours.site_id AND working_hours.type = 'WorkingHours')
 AND (working_hours._valid_from <= dh.date_time_hour AND working_hours._valid_to >= dh.date_time_hour)
;