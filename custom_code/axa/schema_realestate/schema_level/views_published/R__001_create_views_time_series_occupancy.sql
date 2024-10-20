-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW published.timeseries_occupancysensor AS
	SELECT 
    ts.site_id,
    ts.trend_id,
    ts.date_local,
    ts.date_time_local_hour,
    IFF(HOUR(date_time_local_hour) >= COALESCE(working_hours.default_value:hourStart,8) 
    AND HOUR(date_time_local_hour) <  COALESCE(working_hours.default_value:hourEnd, 18), true, false) AS is_working_hour,
    d.is_weekday,
    ts.sum_telemetry_value,
    p.sensor_name,
    p.model_id_capability,
    p.sensor_type,
    p.sensor_id,
    p.space_id,
    p.space_name,
    p.space_type,
    p.seating_capacity,
    p.gross_area,
    p.floor_id,
    p.level_name,
    p.level_id,
	MAX(ts.date_time_local_hour) OVER (PARTITION BY ts.trend_id) AS last_captured_at_local,
	MAX(ts._last_updated_at) OVER () AS last_refreshed_at_utc
FROM transformed.time_series_occupancy ts
JOIN transformed.occupancy_occupancysensor p ON p.site_id = ts.site_id AND p.trend_id = ts.trend_id
JOIN transformed.dates d 
    ON (ts.date_local = d.date)
LEFT JOIN transformed.site_defaults working_hours
    ON (ts.site_id = working_hours.site_id AND working_hours.type = 'WorkingHours')
    AND (working_hours._valid_from <= ts.date_time_local_hour AND working_hours._valid_to >= ts.date_time_local_hour)
;

CREATE OR REPLACE VIEW published.timeseries_peoplecount AS
SELECT 
    ts.site_id,
    ts.trend_id,
    ts.date_local,
    ts.time_local_15min,
    ts.date_time_local_15min,
    ts.date_time_local_hour,
    IFF(HOUR(date_time_local_15min) >= COALESCE(working_hours.default_value:hourStart,8) 
    AND HOUR(date_time_local_15min) <  COALESCE(working_hours.default_value:hourEnd, 18), true, false) AS is_working_hour,
    d.is_weekday,
    ts.telemetry_value,
    p.sensor_name,
    p.model_id_capability,
    p.sensor_type,
    p.sensor_id,
    p.space_id,
    p.space_name,
    p.space_type,
    p.seating_capacity,
    p.gross_area,
    p.floor_id,
    p.level_name,
    p.level_id,
    MAX(ts.timestamp_local) OVER (PARTITION BY ts.trend_id) AS last_captured_at_local,
    MAX(ts.timestamp_utc) OVER (PARTITION BY ts.trend_id) AS last_captured_at_utc,
	MAX(ts.timestamp_utc) OVER () AS last_refreshed_at_utc
FROM transformed.time_series_enriched ts
JOIN transformed.occupancy_peoplecount p ON p.site_id = ts.site_id AND p.trend_id = ts.trend_id
    JOIN transformed.dates d 
      ON (ts.date_local = d.date)
    LEFT JOIN transformed.site_defaults working_hours
        ON (ts.site_id = working_hours.site_id AND working_hours.type = 'WorkingHours')
        AND (working_hours._valid_from <= ts.date_time_local_15min AND working_hours._valid_to >= ts.date_time_local_15min)
WHERE ts.timestamp_utc >= '2022-09-20'
;