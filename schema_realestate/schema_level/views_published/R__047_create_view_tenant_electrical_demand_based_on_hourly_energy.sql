-- ******************************************************************************************************************************
-- Create view
-- This includes lag 7 days; and a join to 4 weeks ago and 52 weeks ago.  Used join instead of lag because non-contiguous data.
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW published.tenant_electrical_demand_based_on_hourly_energy AS
WITH
	cte_tenant_lease AS (
		SELECT DISTINCT
			tenant_unit_id,
			rentable_area,
			COALESCE(lease_start, '2000-01-01') AS lease_start,
			COALESCE(lease_end,   '2999-01-01') AS lease_end
		FROM transformed.tenant_lease
	)
		SELECT
			ts.date_local,
			ts.date_time_local_hour,
			DAYOFWEEKISO(ts.date_local) AS day_of_week,
			CASE WHEN day_of_week > 5 THEN false ELSE true END AS is_weekday,
			IFF(is_weekday = TRUE, 'Weekday', 'Weekend') AS day_of_week_type,
			SUM(ts.hourly_usage) AS power_consumption,
			ema.asset_id, 
			ema.asset_name,
			ema.model_id_asset,
            tenant.tenant_id,
            tenant.tenant_name,
            tenant.tenant_unit_id,
            tenant.tenant_unit_name,
			tl.rentable_area,
			tl.lease_start,
			tl.lease_end,
			ema.site_id,
			ema.site_name,
			ema.building_id,
			ema.building_name,
			ema.customer_id,
			MAX(last_captured_at_local) AS last_captured_at_local,
			MAX(last_captured_at_utc) AS last_captured_at_utc,
			MAX(last_refreshed_at_utc) AS last_refreshed_at_utc,
			MAX(CONVERT_TIMEZONE( 'UTC',ema.time_zone, SYSDATE()) ) AS last_refreshed_at_local
		FROM transformed.agg_electrical_metering_hourly ts
			JOIN transformed.electrical_metering_hierarchy ema
				ON (ts.trend_id = ema.trend_id)
            LEFT JOIN transformed.tenant_served_by_twin tenant
                ON (ema.asset_id = tenant.asset_id)
			LEFT JOIN cte_tenant_lease tl
				ON (tenant.tenant_unit_id = tl.tenant_unit_id)
			   AND (ts.date_local BETWEEN tl.lease_start AND tl.lease_end)
        --WHERE ema.model_id_asset IN ('dtmi:com:willowinc:ElectricalPanelboardMLO;1', 'dtmi:com:willowinc:ElectricalPanelboardMCB;1', 'dtmi:com:willowinc:ElectricalPanelboard;1')
		GROUP BY 
        	ts.date_local,
			ts.date_time_local_hour,
			DAYOFWEEKISO(ts.date_local),
			CASE WHEN day_of_week > 5 THEN false ELSE true END,
			IFF(is_weekday = TRUE, 'Weekday', 'Weekend'),
			ema.asset_id, 
			ema.asset_name,
			ema.model_id_asset,
            tenant.tenant_id,
            tenant.tenant_name,
            tenant.tenant_unit_id,
            tenant.tenant_unit_name,
			tl.rentable_area,
			tl.lease_start,
			tl.lease_end,
			ema.site_id,
			ema.site_name,
			ema.building_id,
			ema.building_name,
			ema.customer_id;
