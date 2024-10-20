-- ------------------------------------------------------------------------------------------------------------------------------
-- create View
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW published.occupancy_building_hourly AS

SELECT
    t.building_id,
    t.building_name,
    t.model_id,
    t.capability_id,
    t.capability_name,
    ts.trend_id,
    ts.date_local,
    ts.datetime_local_hour AS date_local_hour,
    ts.day_name,
    ts.day_of_week,
    ts.end_of_hour_value,
    ts.previous_hour_value,
    ts.hourly_incremental,
    CASE WHEN t.model_id = 'dtmi:com:willowinc:PeopleCountSensor;1' THEN ts.end_of_hour_value
         WHEN t.model_id = 'dtmi:com:willowinc:TotalEnteringPeopleCount;1' THEN ts.hourly_incremental    
         ELSE ts.hourly_incremental 
    END AS occupancy_count,
	ts.is_weekday,
    IFF(HOUR(ts.datetime_local_hour) >= COALESCE(working_hours.default_value:hourStart,8) 
        AND HOUR(ts.datetime_local_hour) <  COALESCE(working_hours.default_value:hourEnd, 18), true, false) AS is_working_hour,
    CONVERT_TIMEZONE( 'UTC',t.time_zone, MAX(ts.last_refreshed_at_utc) OVER () ) AS last_refreshed_at_local
FROM transformed.occupancy_building_hourly ts
JOIN transformed.occupancy_building_twins t ON ts.trend_id = t.trend_id
LEFT JOIN transformed.site_defaults working_hours
  ON (t.site_id = working_hours.site_id AND working_hours.type = 'WorkingHours')
 AND (working_hours._valid_from <= ts.datetime_local_hour AND working_hours._valid_to >= ts.datetime_local_hour)
;