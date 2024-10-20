-- ******************************************************************************************************************************
-- Create view
-- This view overwrites the view published.tenant_electrical_metering_detail in the standard code folder;
-- This is to be used when Utility Account twins have been set up for Utilivisor data
-- Using A so that it will always get deployed - after standard code in case that code has been changed.
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW published.tenant_electrical_metering_detail AS
	WITH cte_hourly AS (
	SELECT
		date_local,
		date_time_local_hour,
		day_of_week,
		is_weekday,
		day_of_week_type,
		AVG(power_consumption) AS avg_hourly_power_consumption,
		asset_id,
		asset_name,
		tenant_id,
		tenant_name,
		tenant_unit_id,
		tenant_unit_name,
		tenant_unit_rentable_area,
		site_id,
		site_name,
		customer_id,
		portfolio_id,
		building_id,
		building_name,
		building_gross_area,
		building_rentable_area,
		building_type,
		MAX(last_captured_at_local) AS last_captured_at_local,
		MAX(last_captured_at_utc) AS last_captured_at_utc,
		MAX(last_refreshed_at_utc) AS last_refreshed_at_utc,
		MAX(last_refreshed_at_local) AS last_refreshed_at_local
	FROM transformed.tenant_energy_utility_account
	GROUP BY 
		date_local,
		date_time_local_hour,
		day_of_week,
		is_weekday,
		day_of_week_type,
		asset_id,
		asset_name,
		tenant_id,
		tenant_name,
		tenant_unit_id,
		tenant_unit_name,
		tenant_unit_rentable_area,
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
	,cte_daily AS (
	SELECT
		date_local,
		day_of_week,
		is_weekday,
		day_of_week_type,
		SUM(avg_hourly_power_consumption) AS daily_usage_kwh,
		asset_id,
		asset_name,
		tenant_id,
		tenant_name,
		tenant_unit_id,
		tenant_unit_name,
		tenant_unit_rentable_area,
		site_id,
		site_name,
		customer_id,
		portfolio_id,
		building_id,
		building_name,
		building_gross_area,
		building_rentable_area,
		building_type,
		MAX(last_captured_at_local) AS last_captured_at_local,
		MAX(last_captured_at_utc) AS last_captured_at_utc,
		MAX(last_refreshed_at_utc) AS last_refreshed_at_utc,
		MAX(last_refreshed_at_local) AS last_refreshed_at_local
	FROM cte_hourly
	GROUP BY 
		date_local,
		day_of_week,
		is_weekday,
		day_of_week_type,
		asset_id,
		asset_name,
		tenant_id,
		tenant_name,
		tenant_unit_id,
		tenant_unit_name,
		tenant_unit_rentable_area,
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
	SELECT ts.*,
		ts_1_week.daily_usage_kwh AS daily_usage_kwh_1_week_ago,
		ts_4_weeeks.daily_usage_kwh AS daily_usage_kwh_4_weeks_ago,
        CASE WHEN ts.daily_usage_kwh = 0 THEN NULL
             ELSE ( ts.daily_usage_kwh - daily_usage_kwh_1_week_ago ) / ts.daily_usage_kwh
        END AS deviation,
        CASE 
            WHEN deviation IS NULL THEN NULL
			WHEN deviation > 0.15 OR deviation < -0.5 THEN 0
            ELSE 1
        END AS energy_rating,
		CASE WHEN ts.daily_usage_kwh = 0 THEN NULL
             ELSE ( ts.daily_usage_kwh - daily_usage_kwh_4_weeks_ago ) / ts.daily_usage_kwh
        END AS deviation_based_on_4_weeks_ago,
        CASE 
            WHEN deviation_based_on_4_weeks_ago IS NULL THEN NULL
			WHEN deviation_based_on_4_weeks_ago > 0.15 OR deviation_based_on_4_weeks_ago < -0.5 THEN 0
            ELSE 1
        END AS energy_rating_based_on_4_weeks_ago
	FROM cte_daily ts
        LEFT JOIN cte_daily ts_1_week
			   ON (ts.asset_id = ts_1_week.asset_id AND DATEADD('week',-1,ts.date_local) = ts_1_week.date_local)
        LEFT JOIN cte_daily ts_4_weeeks
			   ON (ts.asset_id = ts_4_weeeks.asset_id AND DATEADD('week',-4,ts.date_local) = ts_4_weeeks.date_local)
;