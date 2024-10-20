-- ------------------------------------------------------------------------------------------------------------------------------
-- Create View
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.facit_15minute AS 
SELECT 
	ts.date_local,
	ts.time_local_15min,
	ts.date_time_local_15min,
    d.is_weekday,
	ts.is_business_hours,  --redundant now; remove after dashboards are switched over
	ts.is_business_hours AS is_working_hour,
	f.building_id,
	f.building_name,
	ts.site_id,
	ts.trend_id,
	ts.median_value_15min,
	ts.max_value_15min,
	ts_1_weeek.median_value_15min AS median_value_15min_week_ago,
	ts_1_weeek.max_value_15min AS max_value_15min_week_ago,
	f.capability_id,
	f.model_id,
	f.capability_name,
	f.capability_description,
	f.asset_name,
	f.category_name,
	f.level_name,
	f.floor_name,
	f.level_id,
	f.level_number,
	f.floor_sort_order,
	f.space_name,
	f.space_capacity,
	f.seating_capacity,
	f.max_occupancy,
	MAX(ts.date_time_local_15min) OVER (PARTITION BY ts.trend_id) AS last_captured_at_local,
	MAX(ts._last_updated_at) OVER () AS last_refreshed_at_utc,
	CONVERT_TIMEZONE( 'UTC', f.time_zone, MAX(ts._last_updated_at) OVER ()) AS last_refreshed_at_local
FROM transformed.facit_15minute ts
	JOIN transformed.facit_trend_ids f
	  ON (ts.site_id = f.site_id)
	 AND (ts.trend_id = f.trend_id)
    JOIN transformed.dates d 
      ON (ts.date_local = d.date)
LEFT JOIN transformed.facit_15minute ts_1_weeek
	  ON (ts.trend_id = ts_1_weeek.trend_id)
	 AND (DATEADD('week',-1,ts.date_time_local_15min) = ts_1_weeek.date_time_local_15min)
;