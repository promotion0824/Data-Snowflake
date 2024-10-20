-- --------------------------------------------------------------------------------------------
-- Create view that provides data for enterprise comfort dashboard
-- --------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.comfort_hourly_metrics AS
	SELECT
			hourly.asset_id,
			a.asset_name,
			a.excludedFromComfortAnalytics,
			a.building_id,
			a.building_name,
			a.zone_id,
			a.zone_name,
			a.room_id,
			a.room_name,
			a.level_name,
			a.floor_sort_order,
			hourly.date,
			hourly.date_hour_start AS date_time_local_hour,
			hourly.last_captured_at_local,
			a.unit,
			hourly.is_working_hour,
			DAYOFWEEKISO(hourly.date) AS day_of_week,
			CASE WHEN day_of_week > 5 THEN false ELSE true END AS is_weekday,
			hourly.avg_zone_air_temp,
			hourly.min_setpoint_used AS heating_setpoint_used,
			hourly.max_setpoint_used AS cooling_setpoint_used,
			hourly.setpoint_type,
			hourly.offset_type_used,
			hourly.heating_offset_used,
			hourly.cooling_offset_used,
			hourly.sample_count,
			hourly.count_optimum,
			hourly.comfort_score,
			hourly.last_captured_at_utc,
            MAX(hourly._last_updated_at) OVER () AS last_refreshed_at_utc,
			CONVERT_TIMEZONE( 'UTC',a.time_zone, last_refreshed_at_utc) AS last_refreshed_at_local
	FROM transformed.comfort_hourly_metrics hourly
	JOIN transformed.comfort_assets a on hourly.asset_id = a.asset_id
	;