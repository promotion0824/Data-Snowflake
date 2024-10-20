-- ******************************************************************************************************************************
-- Create view
-- Day level thus no is_working_hour column available
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW published.electrical_metering_summary AS

	SELECT DISTINCT
		d.date_local,
		d.day_of_week,
		d.is_weekday,
		d.day_of_week_type,
		CASE WHEN d.daily_usage_kwh IS NULL THEN 'Power' ELSE 'Energy' END AS sensor_type,
		daily_usage_kwh,
		usage_kwh_1_week_ago AS daily_usage_kwh_1_week_ago,
		daily_usage_kwh_4_weeks_ago,
		daily_usage_kwh_52_weeks_ago,
		d.deviation,
        d.energy_rating,
		d.deviation_based_on_4_weeks_ago,
		d.energy_rating_based_on_4_weeks_ago,
		d.asset_id,
		d.asset_name,
		d.asset_display_name,
		d.building_collection,
		d.model_id_asset,
		d.level_2_model_id,
		d.level_2_asset_id,
		d.level_2_asset_name,
		d.level_1_asset_relationship_type,
		d.level_1_model_id,
		d.level_1_asset_id,
		d.level_1_asset_name,
		d.top_level_model_id,
		d.top_level_asset_id,
		d.top_level_asset_name,
		d.site_id,
		d.site_name,
		d.customer_id,
		d.portfolio_id,
		d.building_id,
		d.building_name,
		d.building_gross_area,
		d.building_rentable_area,
		d.building_type,
		d.model_id_asset AS asset_model_class,
		end_of_day_kwh,
		end_of_prev_day_value_kwh,
		daily_usage_kwh_eod_calc,
		last_captured_at_local,
		last_captured_at_utc,
		last_refreshed_at_utc,
		last_refreshed_at_local
FROM published.electrical_metering_detail d
;