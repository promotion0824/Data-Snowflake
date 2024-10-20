---------------------------------------------------------------------------------------
-- Create view that provides data for enterprise comfort dashboard
-- ------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW transformed.return_air_temperature_daily_metrics_v AS
WITH cte_max_refreshed AS (
    SELECT MAX(_last_updated_at) AS last_refreshed_utc FROM transformed.return_air_temperature_hourly_metrics
)
, cte_daily AS (
SELECT 
	date,
	asset_id,
	asset_name,
	excludedFromComfortAnalytics,
	building_id,
	building_name,
	site_id,
	site_name,
	time_zone,
	floor_id,
	floor_name,
	floor_sort_order,
	is_working_hour,
	day_of_week_type,
	COUNT(site_id) count_of_hours,
	-- Only count humidity when there is a value
	SUM(CASE WHEN count_optimum_temp >= 1 AND IFNULL(adj_humidity_count,1) >= 1 THEN 1 
			 WHEN count_optimum_temp IS NULL THEN NULL
			 ELSE 0 
	END) AS optimal_comfort,
	optimal_comfort / count_of_hours * 100 AS comfort_score,
	SUM(CASE WHEN count_optimum_temp >= 1  
			 THEN 1 ELSE 0 
	END) AS optimal_temp,
	optimal_temp/ count_of_hours * 100 AS return_air_temperature_score,
	MAX(last_captured_at_local) AS last_captured_at_local,
	MAX(last_captured_at_utc) AS last_captured_at_utc,
	MAX(last_refreshed_utc) AS last_refreshed_at_utc
FROM transformed.return_air_temperature_hourly_metrics_score,cte_max_refreshed
WHERE 
	avg_return_air_temperature IS NOT NULL
GROUP BY 
	date,
	asset_id,
	asset_name,
	excludedFromComfortAnalytics,
	building_id,
	building_name,
	site_id,
	site_name,
	time_zone,
	floor_id,
	floor_name,
	floor_sort_order,
	is_working_hour,
	day_of_week_type
)
SELECT 
	current_month.date,
	current_month.asset_id,
	current_month.asset_name,
	current_month.building_id,
	current_month.building_name,
	current_month.site_id,
	current_month.site_name,
	current_month.time_zone,
	current_month.excludedFromComfortAnalytics,
	current_month.floor_id,
	current_month.floor_name,
	current_month.floor_sort_order,
	current_month.count_of_hours,
	current_month.optimal_comfort,
	current_month.comfort_score,
	current_month.return_air_temperature_score,
	current_month.is_working_hour,
	current_month.day_of_week_type,
	month_ago.date AS date_month_ago,
	month_ago.count_of_hours AS count_of_hours_month_ago,
	month_ago.optimal_comfort AS optimal_comfort_month_ago,
	month_ago.comfort_score AS comfort_score_month_ago,
	month_ago.return_air_temperature_score AS return_air_temperature_score_month_ago,
	month_ago.is_working_hour AS is_working_hour_month_ago,
	month_ago.day_of_week_type AS day_of_week_type_month_ago,
	current_month.last_captured_at_local,
	current_month.last_captured_at_utc,
	current_month.last_refreshed_at_utc
FROM cte_daily current_month
LEFT JOIN cte_daily month_ago 
	   ON (DATEADD('month',-1,current_month.date) = month_ago.date)
	  AND current_month.is_working_hour = month_ago.is_working_hour 
	  AND (current_month.asset_id = month_ago.asset_id)
;

CREATE OR REPLACE TABLE transformed.return_air_temperature_daily_metrics AS
    SELECT * FROM transformed.return_air_temperature_daily_metrics_v
;