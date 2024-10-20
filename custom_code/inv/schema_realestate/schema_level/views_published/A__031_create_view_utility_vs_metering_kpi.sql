-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW published.utility_vs_metering_kpi AS
WITH cte_billed_energy AS (
  SELECT 
    dateadd(mm,-1,date_local+1) AS start_date,
    ts.date_local AS end_date,
    SUM(ts.telemetry_value) AS billed_energy_consumption,
    ts.building_id,
    ts.building_name,
    ts.site_id,
    ts.site_name
  FROM published.billed_electricity ts
  WHERE 
      ts.model_id_asset = 'dtmi:com:willowinc:ElectricalMeter;1'
  AND ts.model_id = 'dtmi:com:willowinc:BilledActiveElectricalEnergy;1'
  GROUP BY 
    start_date,
    end_date,
    ts.building_id,
    ts.building_name,
    ts.site_id,
    ts.site_name
)
,cte_metering AS (
  SELECT 
      b.start_date,
      b.end_date,
      b.building_id,
      b.building_name,
      m.site_id,
      m.site_name,
      sum(m.daily_usage_kwh) AS metered_consumption_kwh
  FROM published.electrical_metering_detail m
  JOIN cte_billed_energy b
    ON (m.site_id = b.site_id)
   AND (m.date_local BETWEEN b.start_date AND b.end_date)
  WHERE 
      m.model_id_asset IN ('dtmi:com:willowinc:Switchboard;1','dtmi:com:willowinc:Switchgear;1')
  GROUP BY 
      b.start_date,
      b.end_date,
      b.building_id,
      b.building_name,
      m.site_id,
      m.site_name
)
SELECT 
  b.start_date,
  b.end_date,
  b.building_id,
  b.building_name,
  b.site_id,
  b.site_name,
  metered_consumption_kwh,
  billed_energy_consumption,
  ROUND(metered_consumption_kwh/billed_energy_consumption,2) * 100 AS utility_vs_metering_kpi
FROM cte_metering m
JOIN cte_billed_energy b
  ON (m.site_id = b.site_id)
 AND (m.start_date = b.start_date)
;