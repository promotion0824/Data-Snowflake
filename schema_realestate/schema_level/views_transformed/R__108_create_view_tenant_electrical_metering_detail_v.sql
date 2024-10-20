-- ******************************************************************************************************************************
-- Create view
-- This includes lag 7 days; and a join to 4 weeks ago and 52 weeks ago.  Used join instead of lag because non-contiguous data.
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.tenant_electrical_metering_detail_v AS
WITH
	cte_tenant_lease AS (
		SELECT DISTINCT
			tenant_unit_id,
			rentable_area,
			COALESCE(lease_start, '2000-01-01') AS lease_start,
			COALESCE(lease_end,   '2999-01-01') AS lease_end
		FROM transformed.tenant_lease
	)
   ,cte_detail AS (
		SELECT
			ts.date_local,
			ts.trend_id,
			DAYOFWEEKISO(ts.date_local) AS day_of_week,
			CASE WHEN day_of_week > 5 THEN false ELSE true END AS is_weekday,
			IFF(is_weekday = TRUE, 'Weekday', 'Weekend') AS day_of_week_type,
			ts.daily_usage_kwh,
			ts.virtual_daily_usage_kwh,
			LAG(ts.daily_usage_kwh, 7, 0) OVER (PARTITION BY ts.trend_id ORDER BY ts.trend_id, ts.date_local) AS usage_kwh_1_week_ago,
			LAG(ts.virtual_daily_usage_kwh, 7, 0) OVER (PARTITION BY ts.trend_id ORDER BY ts.trend_id, ts.date_local) AS virtual_usage_kwh_1_week_ago,
			ema.asset_id, 
			ema.asset_name,
			ema.model_id_asset,
			ema.floor_id,
			ema.level_name,
			ema.floor_sort_order,
            tenant.tenant_id,
            tenant.tenant_name,
            tenant.tenant_unit_id,
            tenant.tenant_unit_name,
			tl.rentable_area,
			tl.lease_start,
			tl.lease_end,
			ts.site_id,
			ema.site_name,
			ema.customer_id,
			ema.portfolio_id,
			ema.building_id,
			ema.building_name,
			ema.building_gross_area,
			ema.building_rentable_area,
			ema.building_type,
			end_of_day_kwh,
			end_of_prev_day_value_kwh,
			daily_usage_kwh_eod_calc,
			last_captured_at_local,
			last_captured_at_utc,
			last_refreshed_at_utc,
			CONVERT_TIMEZONE( 'UTC',ema.time_zone, SYSDATE()) AS last_refreshed_at_local
		FROM transformed.agg_electrical_metering_daily ts
			JOIN transformed.electrical_metering_hierarchy ema
				ON (ts.trend_id = ema.trend_id)
            LEFT JOIN transformed.tenant_served_by_twin tenant
                ON (ema.asset_id = tenant.asset_id)
			LEFT JOIN cte_tenant_lease tl
				ON (tenant.tenant_unit_id = tl.tenant_unit_id)
			   AND (ts.date_local BETWEEN tl.lease_start AND tl.lease_end)
        WHERE ema.model_id_asset IN ('dtmi:com:willowinc:ElectricalPanelboardMLO;1', 'dtmi:com:willowinc:ElectricalPanelboardMCB;1', 'dtmi:com:willowinc:ElectricalPanelboard;1')
	)
	SELECT
		det.date_local,
		ts_4_weeeks.date_local AS date_local_4_weeeks_ago,
		DAYOFWEEKISO(det.date_local) AS day_of_week,
		CASE WHEN day_of_week > 5 THEN false ELSE true END AS is_weekday,
		IFF(is_weekday = TRUE, 'Weekday', 'Weekend') AS day_of_week_type,
		COALESCE(NULLIFZERO(SUM(det.daily_usage_kwh)),NULLIFZERO(SUM(det.virtual_daily_usage_kwh))) AS unified_daily_usage_kwh,
		COALESCE(NULLIFZERO(SUM(det.usage_kwh_1_week_ago)),NULLIFZERO(SUM(det.virtual_usage_kwh_1_week_ago))) AS unified_1_week_ago_daily_kwh,
		SUM(det.daily_usage_kwh) AS sum_daily_usage_kwh,
        sum_daily_usage_kwh AS daily_usage_kwh,
		SUM(det.virtual_daily_usage_kwh) AS power_based_daily_usage_kwh,
        SUM(det.usage_kwh_1_week_ago) AS sum_usage_kwh_1_week_ago,
        sum_usage_kwh_1_week_ago AS usage_kwh_1_week_ago,
        SUM(det.virtual_usage_kwh_1_week_ago) AS power_based_daily_usage_kwh_1_week_ago,
		SUM(ts_4_weeeks.daily_usage_kwh) AS daily_usage_kwh_4_weeks_ago,
		SUM(ts_4_weeeks.virtual_daily_usage_kwh) AS power_based_daily_usage_kwh_4_weeks_ago,
		SUM(ts_52_weeks.daily_usage_kwh) AS daily_usage_kwh_52_weeks_ago,
		SUM(ts_52_weeks.virtual_daily_usage_kwh) AS power_based_daily_usage_kwh_52_weeks_ago,
        CASE WHEN sum_daily_usage_kwh = 0 THEN NULL
             ELSE ( sum_daily_usage_kwh - sum_usage_kwh_1_week_ago ) / sum_daily_usage_kwh
        END AS deviation,
        CASE 
            WHEN deviation IS NULL THEN NULL
			WHEN deviation > 0.15 OR deviation < -0.5 THEN 0
            ELSE 1
        END AS energy_rating,
		COALESCE(NULLIFZERO(daily_usage_kwh_4_weeks_ago),NULLIFZERO(power_based_daily_usage_kwh_4_weeks_ago)) AS unified_4_week_ago_daily_kwh,
		CASE WHEN unified_daily_usage_kwh = 0 THEN NULL
             ELSE ( unified_daily_usage_kwh - unified_4_week_ago_daily_kwh ) / unified_daily_usage_kwh
        END AS deviation_based_on_4_weeks_ago,
        CASE 
            WHEN deviation_based_on_4_weeks_ago IS NULL THEN NULL
			WHEN deviation_based_on_4_weeks_ago > 0.15 OR deviation_based_on_4_weeks_ago < -0.5 THEN 0
            ELSE 1
        END AS energy_rating_based_on_4_weeks_ago,
		det.asset_id,
		det.asset_name,
		det.model_id_asset,
		det.floor_id,
		det.level_name,
		det.floor_sort_order,
        det.tenant_id,
        det.tenant_name,
        det.tenant_unit_id,
        det.tenant_unit_name,
		det.lease_start,
		det.lease_end,
		MAX(det.rentable_area) AS tenant_unit_rentable_area,
		tenant_unit_rentable_area / det.building_rentable_area AS pct_of_total_leased_space,
		det.site_id,
		det.site_name,
		det.customer_id,
		det.portfolio_id,
		det.building_id,
		det.building_name,
		det.building_gross_area,
		det.building_rentable_area,
		det.building_type,
		MAX(det.end_of_day_kwh) AS end_of_day_kwh,
		MAX(det.end_of_prev_day_value_kwh) AS end_of_prev_day_value_kwh,
		SUM(det.daily_usage_kwh_eod_calc) AS daily_usage_kwh_eod_calc,
		MAX(det.last_captured_at_local) AS last_captured_at_local,
		MAX(det.last_captured_at_utc) AS last_captured_at_utc,
		MAX(det.last_refreshed_at_utc) AS last_refreshed_at_utc,
		MAX(last_refreshed_at_local) AS last_refreshed_at_local
	FROM cte_detail det
		LEFT JOIN transformed.site_defaults d
		   ON (det.site_id = d.site_id)
		  AND (d.type ='EnergyDataStartDate')
		LEFT JOIN transformed.site_defaults d2 
		   ON (det.site_id = d2.site_id)
		  AND (d2.type ='AssetStartDate')
		  AND (det.asset_id = d2.default_value:AssetId::STRING OR d2.default_value:AssetId IS NULL)
        LEFT JOIN transformed.agg_electrical_metering_daily ts_4_weeeks
			   ON (det.trend_id = ts_4_weeeks.trend_id AND DATEADD('week',-4,det.date_local) = ts_4_weeeks.date_local)
			  AND ts_4_weeeks.date_local >= COALESCE(d2.default_value:AssetStartDate, d.default_value:SiteStartDate, '2019-01-01')
        LEFT JOIN transformed.agg_electrical_metering_daily ts_52_weeks
			   ON (det.trend_id = ts_52_weeks.trend_id AND DATEADD('week',-52,det.date_local) = ts_52_weeks.date_local)
			  AND ts_52_weeks.date_local >= COALESCE(d2.default_value:AssetStartDate, d.default_value:SiteStartDate, '2019-01-01')	   
	GROUP BY
		det.date_local,
		date_local_4_weeeks_ago,
		det.day_of_week,
		det.is_weekday,
		day_of_week_type,
		det.asset_id,
		det.asset_name,
		det.model_id_asset,
		det.floor_id,
		det.level_name,
		det.floor_sort_order,
        det.tenant_id,
        det.tenant_name,
        det.tenant_unit_id,
        det.tenant_unit_name,
		det.lease_start,
		det.lease_end,
		det.site_id,
		det.site_name,
		det.customer_id,
		det.portfolio_id,
		det.building_id,
		det.building_name,
		det.building_gross_area,
		det.building_rentable_area,
		det.building_type
;

CREATE OR REPLACE TABLE transformed.tenant_electrical_metering_detail AS SELECT * FROM transformed.tenant_electrical_metering_detail_v;