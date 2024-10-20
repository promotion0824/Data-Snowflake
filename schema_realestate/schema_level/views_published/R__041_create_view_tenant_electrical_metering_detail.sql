-- ******************************************************************************************************************************
-- Create view
-- This includes lag 7 days; and a join to 4 weeks ago and 52 weeks ago.  Used join instead of lag because non-contiguous data.
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW published.tenant_electrical_metering_detail AS
	SELECT
		date_local,
		day_of_week,
		is_weekday,
		day_of_week_type,
		unified_daily_usage_kwh,
		unified_1_week_ago_daily_kwh,
		daily_usage_kwh,
		power_based_daily_usage_kwh,
        usage_kwh_1_week_ago,
        power_based_daily_usage_kwh_1_week_ago,
		daily_usage_kwh_4_weeks_ago,
		power_based_daily_usage_kwh_4_weeks_ago,
		daily_usage_kwh_52_weeks_ago,
		power_based_daily_usage_kwh_52_weeks_ago,
        deviation,
		energy_rating,
		deviation_based_on_4_weeks_ago,
		energy_rating_based_on_4_weeks_ago,
		asset_id,
		asset_name,
		model_id_asset,
		floor_id,
		level_name,
		floor_sort_order,
		tenant_id,
		tenant_name,
		tenant_unit_id,
		tenant_unit_name,
		tenant_unit_rentable_area,
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
	FROM transformed.tenant_electrical_metering_detail emd
		LEFT JOIN transformed.site_defaults d
			   ON (emd.site_id = d.site_id)
              AND (d.type ='EnergyDataStartDate')
		LEFT JOIN transformed.site_defaults d2 
		       ON (emd.site_id = d2.site_id)
              AND (d2.type ='AssetStartDate')
              AND (emd.asset_id = d2.default_value:AssetId::STRING OR d2.default_value:AssetId IS NULL)
	WHERE emd.date_local >= COALESCE(d2.default_value:AssetStartDate, d.default_value:SiteStartDate, '2019-01-01')
;