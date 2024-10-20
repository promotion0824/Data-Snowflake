-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW published.utility_vs_metering_kpi AS
WITH cte_billed_energy AS (
  SELECT 
    COALESCE(LAG(ts.date_local, 1) OVER (PARTITION BY ts.site_id ORDER BY ts.date_local) + 1, dateadd(mm,-1,date_local) + 1) AS start_date,
    ts.date_local AS end_date,
    ts.telemetry_value AS billed_energy_consumption,
    ts.site_id,
    ts.site_name,
    ts.building_id,
    ts.building_name
  FROM published.billed_electricity ts
  WHERE 
      ts.model_id_asset = 'dtmi:com:willowinc:ElectricalMeter;1'
  AND ts.model_id = 'dtmi:com:willowinc:BilledActiveElectricalEnergy;1'
)
,cte_metering AS (
  SELECT 
      b.start_date,
      b.end_date,
      m.site_id,
      m.site_name,
      m.building_id,
      m.building_name,
      sum(m.daily_usage_kwh) AS metered_consumption_kwh
  FROM published.electrical_metering_detail m
  JOIN cte_billed_energy b
    ON (m.site_id = b.site_id)
   AND (m.date_local BETWEEN b.start_date AND b.end_date)
  GROUP BY 
      b.start_date,
      b.end_date,
      m.site_id,
      m.site_name,
      m.building_id,
      m.building_name
)
SELECT 
  b.start_date,
  b.end_date,
  b.site_id,
  b.site_name,
  b.building_id,
  b.building_name,
  metered_consumption_kwh,
  billed_energy_consumption,
  ROUND(metered_consumption_kwh/billed_energy_consumption,2) * 100 AS utility_vs_metering_kpi
FROM cte_metering m
JOIN cte_billed_energy b
  ON (m.site_id = b.site_id)
 AND (m.start_date = b.start_date)
;