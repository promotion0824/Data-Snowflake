-- ------------------------------------------------------------------------------------------------------------------------------
-- create View
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW published.occupancy_space_hourly AS

SELECT 
    ts.date_local,
    ts.datetime_local_hour,
    ts.day_name,
    ts.day_of_week,
    ts.hour_num,
    ts.external_id,
    ts.occupancy_count,
    t.building_id,
    t.building_name,
    t.model_id,
    t.trend_id,
    t.capability_id,
    t.model_id_asset,
    t.asset_id,
    t.asset_name,
    t.room,
    t.occupancy_zone,
    t.zone_type,
    t.max_occupancy,
    t.seating_capacity,
    t.level_id,
    t.level_name,
    t.floor_sort_order,
	  ts.is_weekday,
    IFF(HOUR(ts.datetime_local_hour) >= COALESCE(working_hours.default_value:hourStart,8) 
        AND HOUR(ts.datetime_local_hour) <  COALESCE(working_hours.default_value:hourEnd, 18), true, false) AS is_working_hour,
    t.time_zone,
    ts.last_captured_at_ut,
    ts.last_refreshed_at_utc,
    CONVERT_TIMEZONE( 'UTC',t.time_zone, MAX(ts.last_refreshed_at_utc) OVER () ) AS last_refreshed_at_local
FROM transformed.occupancy_space_hourly ts
JOIN transformed.occupancy_space_twins t 
  ON (ts.external_id = t.external_id OR ts.trend_id = t.trend_id)
LEFT JOIN transformed.site_defaults working_hours
  ON (t.site_id = working_hours.site_id AND working_hours.type = 'WorkingHours')
 AND (working_hours._valid_from <= ts.datetime_local_hour AND working_hours._valid_to >= ts.datetime_local_hour)
;