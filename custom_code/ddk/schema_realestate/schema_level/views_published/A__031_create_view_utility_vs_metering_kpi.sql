-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW published.utility_vs_metering_kpi AS
WITH cte_metering AS (
SELECT 
    date_trunc('month',date_local) as date_local,
    building_id,
    building_name,
    site_id,
    site_name,
    sum(daily_usage_kwh) AS daily_usage_kwh
FROM published.electrical_metering_detail
GROUP BY 
    date_trunc('month',date_local),
    building_id,
    building_name,
    site_id,
    site_name
)
SELECT 
    date_trunc('month', be.date_local) as date_local,
    SUM(telemetry_value) AS billed_energy_consumption,
    SUM(daily_usage_kwh) AS metered_consumption_kwh,
    ROUND(metered_consumption_kwh/billed_energy_consumption,2) * 100 AS utility_vs_metering_kpi,
    be.unit AS unit_from_billed,
    building_id,
    building_name,
    be.site_id
FROM transformed.billed_electricity be
LEFT JOIN cte_metering m 
  ON (m.date_local = date_trunc('month', be.date_local) )
 AND (be.site_id = m.site_id)
WHERE --be.model_id_asset = 'dtmi:com:willowinc:Building;1'
      be.model_id = 'dtmi:com:willowinc:BilledActiveElectricalEnergy;1'
  AND be.unit IN ('kW','kWh')
GROUP BY be.date_local, be.asset_name, be.unit, be.site_id, building_id,
    building_name;