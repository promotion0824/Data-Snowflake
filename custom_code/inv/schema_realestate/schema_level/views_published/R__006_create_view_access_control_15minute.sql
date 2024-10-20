-- ------------------------------------------------------------------------------------------------------------------------------
-- Create View
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.access_control_15minute AS 
SELECT 
	ts.date_local,
	ts.time_local_15min,
	ts.date_time_local_15min,
    IFF(HOUR(date_time_local_15min) >= COALESCE(working_hours.default_value:hourStart,8) 
    AND HOUR(date_time_local_15min) <  COALESCE(working_hours.default_value:hourEnd, 18), true, false) AS is_working_hour,
    d.is_weekday,
	ts.is_business_hours,
	cc.building_id,
	cc.building_name,
	cc.site_name,
	ts.site_id,
	ts.trend_id,
	ts.last_15min_value AS telemetry_value,
	ts.prev_15min_value,
	ts.diff_to_prev,
	cc.capability_id,
	cc.model_id,
	cc.capability_name,
	cc.capability_description,
	cc.asset_name,
	cc.category_name,
	cc.level_name,
	cc.level_number,
	cc.level_code,
	cc.floor_sort_order,
	MAX(ts.date_time_local_15min) OVER (PARTITION BY ts.trend_id) AS last_captured_at_local,
	MAX(ts._last_updated_at) OVER () AS last_refreshed_at_utc,
	CONVERT_TIMEZONE( 'UTC',cc.time_zone, MAX(ts._last_updated_at) OVER ()) AS last_refreshed_at_local
FROM transformed.access_control_15minute ts
	JOIN transformed.access_control_trend_ids cc 
	  ON (ts.site_id = cc.site_id)
	 AND (ts.trend_id = cc.trend_id)
    JOIN transformed.dates d 
      ON (ts.date_local = d.date)
    LEFT JOIN transformed.site_defaults working_hours
      ON (ts.site_id = working_hours.site_id AND working_hours.type = 'WorkingHours')
     AND (working_hours._valid_from <= ts.date_time_local_15min AND working_hours._valid_to >= ts.date_time_local_15min)
;