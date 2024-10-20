-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW published.electrical_metering_detail AS
-- Get agg Switchboard data for Sofi Stadium
	WITH cte_switchboard_agg AS (
	SELECT
			date_local,
			day_of_week,
			is_weekday,
			day_of_week_type,
			SUM(unified_daily_usage_kwh) AS agg_unified_daily_usage_kwh,
			SUM(unified_1_week_ago_daily_kwh) AS agg_unified_1_week_ago_daily_kwh,
			SUM(daily_usage_kwh) AS agg_daily_usage_kwh,
			SUM(virtual_daily_usage_kwh) AS agg_virtual_daily_usage_kwh,
			SUM(usage_kwh_1_week_ago) AS agg_usage_kwh_1_week_ago,
			SUM(virtual_usage_kwh_1_week_ago) AS agg_virtual_usage_kwh_1_week_ago,
			SUM(daily_usage_kwh_4_weeks_ago) AS agg_daily_usage_kwh_4_weeks_ago,
			SUM(virtual_daily_usage_kwh_4_weeks_ago) AS agg_virtual_daily_usage_kwh_4_weeks_ago,
			SUM(daily_usage_kwh_52_weeks_ago) AS agg_daily_usage_kwh_52_weeks_ago,
			SUM(virtual_daily_usage_kwh_52_weeks_ago) AS agg_virtual_daily_usage_kwh_52_weeks_ago,
			'Other' AS asset_id,
			'Other' AS asset_name,
			'Other' AS asset_display_name,
			'Other' AS model_id_asset,
			'Other' AS level_2_model_id,
			'Other' AS level_2_asset_id,
			'Other' AS level_2_asset_name,
			'Other' AS level_1_asset_relationship_type,
			'Other' AS level_1_model_id,
			'Other' AS level_1_asset_id,
			'Other' AS level_1_asset_name,
			'Other' AS top_level_model_id,
			'Other' AS top_level_asset_id,
			'Other' AS top_level_asset_name,
			site_id,
			site_name,
			customer_id,
			portfolio_id,
			building_id,
			building_name,
			building_gross_area,
			building_rentable_area,
			building_type,
			NULL AS end_of_day_kwh,
			NULL AS end_of_prev_day_value_kwh,
			NULL AS daily_usage_kwh_eod_calc,
			MAX(last_captured_at_local) AS last_captured_at_local,
			MAX(last_captured_at_utc) AS last_captured_at_utc,
			MAX(last_refreshed_at_utc) AS last_refreshed_at_utc,
			MAX(last_refreshed_at_local) AS last_refreshed_at_local
	FROM transformed.electrical_metering_detail2_v
	WHERE model_id_asset = 'dtmi:com:willowinc:Switchboard;1'
	  AND (site_id = 'acce2181-e847-442b-98f7-fbcc70d4d584'
	  AND IFNULL(asset_display_name,'') NOT IN ('YouTube Theater'))
	GROUP BY 
			date_local,
			day_of_week,
			is_weekday,
			day_of_week_type,
			site_id,
			site_name,
			customer_id,
			portfolio_id,
			building_id,
			building_name,
			building_gross_area,
			building_rentable_area,
			building_type
)
-- Get agg Switchgear data for Sofi Stadium
,cte_switchgear_agg AS (
	SELECT
			date_local,
			day_of_week,
			is_weekday,
			day_of_week_type,
			SUM(unified_daily_usage_kwh) AS agg_unified_daily_usage_kwh,
			SUM(unified_1_week_ago_daily_kwh) AS agg_unified_1_week_ago_daily_kwh,
			SUM(daily_usage_kwh) AS agg_daily_usage_kwh,
			SUM(virtual_daily_usage_kwh) AS agg_virtual_daily_usage_kwh,
			SUM(usage_kwh_1_week_ago) AS agg_usage_kwh_1_week_ago,
			SUM(virtual_usage_kwh_1_week_ago) AS agg_virtual_usage_kwh_1_week_ago,
			SUM(daily_usage_kwh_4_weeks_ago) AS agg_daily_usage_kwh_4_weeks_ago,
			SUM(virtual_daily_usage_kwh_4_weeks_ago) AS agg_virtual_daily_usage_kwh_4_weeks_ago,
			SUM(daily_usage_kwh_52_weeks_ago) AS agg_daily_usage_kwh_52_weeks_ago,
			SUM(virtual_daily_usage_kwh_52_weeks_ago) AS agg_virtual_daily_usage_kwh_52_weeks_ago,
			'Other' AS asset_id,
			'Other' AS asset_name,
			'Other' AS asset_display_name,
			'Other' AS model_id_asset,
			'Other' AS level_2_model_id,
			'Other' AS level_2_asset_id,
			'Other' AS level_2_asset_name,
			'Other' AS level_1_asset_relationship_type,
			'Other' AS level_1_model_id,
			'Other' AS level_1_asset_id,
			'Other' AS level_1_asset_name,
			'Other' AS top_level_model_id,
			'Other' AS top_level_asset_id,
			'Other' AS top_level_asset_name,
			site_id,
			site_name,
			customer_id,
			portfolio_id,
			building_id,
			building_name,
			building_gross_area,
			building_rentable_area,
			building_type,
			NULL AS end_of_day_kwh,
			NULL AS end_of_prev_day_value_kwh,
			NULL AS daily_usage_kwh_eod_calc,
			MAX(last_captured_at_local) AS last_captured_at_local,
			MAX(last_captured_at_utc) AS last_captured_at_utc,
			MAX(last_refreshed_at_utc) AS last_refreshed_at_utc,
			MAX(last_refreshed_at_local) AS last_refreshed_at_local
	FROM transformed.electrical_metering_detail2_v
	WHERE model_id_asset = 'dtmi:com:willowinc:Switchgear;1'
	  AND (site_id = 'acce2181-e847-442b-98f7-fbcc70d4d584'
	  AND IFNULL(asset_display_name,'') NOT IN ('YouTube Theater'))
	GROUP BY 
			date_local,
			day_of_week,
			is_weekday,
			day_of_week_type,
			site_id,
			site_name,
			customer_id,
			portfolio_id,
			building_id,
			building_name,
			building_gross_area,
			building_rentable_area,
			building_type
)
-- Get Switchgear - Switchboard for Sofi Stadium;
SELECT 
			gear.date_local,
			gear.day_of_week,
			gear.is_weekday,
			gear.day_of_week_type,
			GREATEST(0, gear.agg_unified_daily_usage_kwh - board.agg_unified_daily_usage_kwh) AS unified_daily_usage_kwh,
			GREATEST(0, gear.agg_unified_1_week_ago_daily_kwh - board.agg_unified_1_week_ago_daily_kwh) AS unified_1_week_ago_daily_kwh,
			GREATEST(0, gear.agg_daily_usage_kwh - board.agg_daily_usage_kwh) AS daily_usage_kwh,
			GREATEST(0, gear.agg_virtual_daily_usage_kwh - board.agg_virtual_daily_usage_kwh) AS virtual_daily_usage_kwh,
			GREATEST(0, gear.agg_usage_kwh_1_week_ago - board.agg_usage_kwh_1_week_ago) AS usage_kwh_1_week_ago,
			GREATEST(0, gear.agg_virtual_usage_kwh_1_week_ago - board.agg_virtual_usage_kwh_1_week_ago) AS virtual_usage_kwh_1_week_ago,
			GREATEST(0, gear.agg_daily_usage_kwh_4_weeks_ago - board.agg_daily_usage_kwh_4_weeks_ago) AS daily_usage_kwh_4_weeks_ago,
			GREATEST(0, gear.agg_virtual_daily_usage_kwh_4_weeks_ago - board.agg_virtual_daily_usage_kwh_4_weeks_ago) AS virtual_daily_usage_kwh_4_weeks_ago,
			GREATEST(0, gear.agg_daily_usage_kwh_52_weeks_ago - board.agg_daily_usage_kwh_52_weeks_ago) AS daily_usage_kwh_52_weeks_ago,
			GREATEST(0, gear.agg_virtual_daily_usage_kwh_52_weeks_ago - board.agg_virtual_daily_usage_kwh_52_weeks_ago) AS virtual_daily_usage_kwh_52_weeks_ago,
			CASE WHEN daily_usage_kwh = 0 THEN NULL
				 ELSE ( daily_usage_kwh - usage_kwh_1_week_ago ) / daily_usage_kwh
			END AS deviation,
			CASE 
				WHEN deviation IS NULL THEN NULL
				WHEN deviation > 0.15 OR deviation < -0.5 THEN 0
				ELSE 1
			END AS energy_rating,
			COALESCE(NULLIFZERO(daily_usage_kwh_4_weeks_ago),NULLIFZERO(virtual_daily_usage_kwh_4_weeks_ago)) AS unified_4_week_ago_daily_kwh,
			CASE WHEN unified_daily_usage_kwh = 0 THEN NULL
				 ELSE ( unified_daily_usage_kwh - unified_4_week_ago_daily_kwh ) / unified_daily_usage_kwh
			END AS deviation_based_on_4_weeks_ago,
			CASE 
				WHEN deviation_based_on_4_weeks_ago IS NULL THEN NULL
				WHEN deviation_based_on_4_weeks_ago > 0.15 OR deviation_based_on_4_weeks_ago < -0.5 THEN 0
				ELSE 1
			END AS energy_rating_based_on_4_weeks_ago,
			gear.asset_id,
			gear.asset_name,
			gear.asset_display_name,
			gear.model_id_asset,
			gear.level_2_model_id,
			gear.level_2_asset_id,
			gear.level_2_asset_name,
			gear.level_1_asset_relationship_type,
			gear.level_1_model_id,
			gear.level_1_asset_id,
			gear.level_1_asset_name,
			gear.top_level_model_id,
			gear.top_level_asset_id,
			gear.top_level_asset_name,
			gear.site_id,
			gear.site_name,
			gear.customer_id,
			gear.portfolio_id,
			gear.building_id,
			gear.building_name,
			gear.building_gross_area,
			gear.building_rentable_area,
			gear.building_type,
			gear.end_of_day_kwh,
			gear.end_of_prev_day_value_kwh,
			gear.daily_usage_kwh_eod_calc,
			gear.last_captured_at_local,
			gear.last_captured_at_utc,
			gear.last_refreshed_at_utc,
			gear.last_refreshed_at_local
FROM cte_switchgear_agg gear
LEFT JOIN cte_switchboard_agg board
	 ON gear.date_local = board.date_local
	AND gear.site_id = board.site_id

-- UNION of switchgear - switchboard; plus all other switchboard details
UNION ALL

SELECT 
			date_local,
			day_of_week,
			is_weekday,
			day_of_week_type,
			unified_daily_usage_kwh,
			unified_1_week_ago_daily_kwh,
			daily_usage_kwh,
			virtual_daily_usage_kwh,
			usage_kwh_1_week_ago,
			virtual_usage_kwh_1_week_ago,
			daily_usage_kwh_4_weeks_ago,
			virtual_daily_usage_kwh_4_weeks_ago,
			daily_usage_kwh_52_weeks_ago,
			virtual_daily_usage_kwh_52_weeks_ago,
			deviation,
			energy_rating,
			unified_4_week_ago_daily_kwh,
			deviation_based_on_4_weeks_ago,
			energy_rating_based_on_4_weeks_ago,
			asset_id,
			asset_name,
			asset_display_name,
			model_id_asset,
			level_2_model_id,
			level_2_asset_id,
			level_2_asset_name,
			level_1_asset_relationship_type,
			level_1_model_id,
			level_1_asset_id,
			level_1_asset_name,
			top_level_model_id,
			top_level_asset_id,
			top_level_asset_name,
			site_id,
			site_name,
			customer_id,
			portfolio_id,
			building_id,
			building_name,
			building_gross_area,
			building_rentable_area,
			building_type,
			end_of_day_kwh,
			end_of_prev_day_value_kwh,
			daily_usage_kwh_eod_calc,
			last_captured_at_local,
			last_captured_at_utc,
			last_refreshed_at_utc,
			last_refreshed_at_local
FROM transformed.electrical_metering_detail2_v  
	WHERE (model_id_asset = 'dtmi:com:willowinc:Switchboard;1' and site_id = 'acce2181-e847-442b-98f7-fbcc70d4d584')
	   OR site_id != 'acce2181-e847-442b-98f7-fbcc70d4d584'
	   OR IFNULL(asset_display_name,'') = 'YouTube Theater'
;
