---------------------------------------------------------------------------------------
-- Create view that provides data for enterprise comfort dashboard
-- ------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.return_air_temperature_daily_metrics AS

SELECT 
	date,
	is_working_hour,
	day_of_week_type,
	CASE WHEN day_of_week_type = 'Weekday' THEN 1 ELSE 0 END AS is_weekday,
	asset_id,
	asset_name,
	excludedFromComfortAnalytics,
	building_id,
	building_name,
	site_id,
	site_name,
	floor_id,
	floor_name,
	floor_sort_order,
	count_of_hours,
	optimal_comfort,
	comfort_score,
	return_air_temperature_score,
	comfort_score_month_ago,
	return_air_temperature_score_month_ago,
	date_month_ago,
	is_working_hour_month_ago,
	day_of_week_type_month_ago,
	count_of_hours_month_ago,
	optimal_comfort_month_ago,
	last_captured_at_local,
	last_captured_at_utc,
	last_refreshed_at_utc,
	CONVERT_TIMEZONE( 'UTC',time_zone, last_refreshed_at_utc) AS last_refreshed_at_local
FROM transformed.return_air_temperature_daily_metrics
;
