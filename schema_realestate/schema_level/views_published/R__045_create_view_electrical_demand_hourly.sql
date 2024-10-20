-- ------------------------------------------------------------------------------------------------------------------------------
-- Create electrical_demand_hourly
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.electrical_demand_hourly AS
WITH cte_assets AS (
	  SELECT DISTINCT
        assets.asset_id,
        assets.asset_name,
        assets.model_id_asset,
        assets.building_id,
        assets.building_name,
        assets.site_id,
        assets.site_name,
        assets.floor_id,
        assets.unit,
        assets.level_name AS floor_name,
        assets.floor_sort_order,
        tnt.tenant_name,
        tnt.tenant_id,
        tnt.tenant_unit_name
	  FROM transformed.electrical_metering_hierarchy assets
      LEFT JOIN transformed.tenant_served_by_twin tnt
        ON (assets.asset_id = tnt.asset_id)
	  )

SELECT 
        date_local,
        date_time_local_hour,
        daily_peak_demand_building,
        is_peak_hour,
        building_peak_hour,
        hourly_power_consumption,
        values_count,
        hourly.asset_id,
        assets.asset_name,
        assets.model_id_asset,
        assets.building_id,
        assets.building_name,
        assets.site_id,
        assets.site_name,
        assets.floor_id,
        assets.unit,
        floor_name,
        assets.floor_sort_order,
        assets.tenant_name,
        tenant_id,
        assets.tenant_unit_name,
        last_refreshed_at_local
FROM transformed.agg_electrical_demand_hourly hourly
JOIN cte_assets assets 
  ON (hourly.asset_id = assets.asset_id)
WHERE hourly_power_consumption > 0
;
