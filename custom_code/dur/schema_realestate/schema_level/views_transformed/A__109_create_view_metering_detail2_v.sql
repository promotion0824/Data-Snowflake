-- ******************************************************************************************************************************
-- Create view
-- This includes lag 7 days; and a join to 4 weeks ago and 52 weeks ago.  Used join instead of lag because non-contiguous data.
-- Day level thus no is_working_hour column available
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.electrical_metering_detail2_v AS
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
		building_collection,
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
		emd.site_id,
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
	FROM transformed.electrical_metering_detail emd
		LEFT JOIN transformed.site_defaults d
			   ON (emd.site_id = d.site_id)
              AND (d.type ='EnergyDataStartDate')
		LEFT JOIN transformed.site_defaults d2 
		       ON (emd.site_id = d2.site_id)
              AND (d2.type ='AssetStartDate')
              AND (emd.asset_id = d2.default_value:AssetId::STRING OR d2.default_value:AssetId IS NULL)
	WHERE emd.date_local >= COALESCE(d2.default_value:AssetStartDate, d.default_value:SiteStartDate, '2019-01-01')
	  AND (top_level_model_id IN ('dtmi:com:willowinc:Switchboard;1','dtmi:com:willowinc:Switchgear;1')
	   OR  model_id_asset IN ('dtmi:com:willowinc:ElectricalMeter;1'))
;