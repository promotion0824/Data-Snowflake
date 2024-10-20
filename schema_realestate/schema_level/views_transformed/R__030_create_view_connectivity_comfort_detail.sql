-- ------------------------------------------------------------------------------------------------------------------------------
-- Create view
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW transformed.connectivity_comfort_detail AS 
--  Actual vs Expected at the 15 Minute level; this is why we use the multiplier of 4
WITH cte_telemetry AS (
SELECT 
    asset_id,
    date_time_local_15min,
    date_time_local_hour,
    date_local,
    COUNT(DISTINCT ts.trend_id) AS trend_count
FROM transformed.time_series_enriched ts
JOIN transformed.capabilities_assets ca
   ON (ts.site_id = ca.site_id)
  AND (ts.trend_id = ca.trend_id)
WHERE date_local >= last_day(current_date - interval '30 days')
  AND model_id IN ('dtmi:com:willowinc:ZoneAirTemperatureSensor;1','dtmi:com:willowinc:ZoneAirTemperatureSetpoint;1','dtmi:com:willowinc:TargetZoneAirTemperatureSetpoint;1')
  AND asset_id IN (SELECT distinct asset_id FROM transformed.zone_air_temp_assets)
GROUP BY 
    asset_id,
    date_time_local_15min,
    date_time_local_hour,
    date_local
)
, cte_assets AS (
SELECT 
    ca.asset_id,
    ca.asset_name,
    ca.model_id_asset,
    s.site_id,
    s.name AS site_name,
    COUNT(*) AS sensors,
    ARRAY_AGG(ca.model_id) WITHIN GROUP (ORDER BY ca.model_id ASC) AS capability_models
FROM transformed.capabilities_assets ca
JOIN transformed.sites s 
  ON (ca.site_id = s.site_id)
WHERE ca.model_id IN ('dtmi:com:willowinc:ZoneAirTemperatureSensor;1','dtmi:com:willowinc:ZoneAirTemperatureSetpoint;1','dtmi:com:willowinc:TargetZoneAirTemperatureSetpoint;1')
GROUP BY
    ca.asset_id,
    ca.asset_name,
    ca.model_id_asset,
    s.site_id,
    s.name
)
SELECT
    dh.date,
    dh.date_time_local_hour,
    dh.date_time_local_15min,
    cte_assets.asset_id,
    cte_assets.asset_name,
    cte_assets.model_id_asset,
    cte_assets.site_id,
    cte_assets.site_name,
    MAX(cte_assets.sensors) * 4 AS trend_count_expected,
    SUM(cte_telemetry.trend_count) AS trend_count_actual,
    cte_assets.capability_models,
    MAX(cte_assets.sensors) AS num_sensors
FROM transformed.date_hour_15min dh
CROSS JOIN cte_assets
LEFT JOIN cte_telemetry 
  ON (dh.date_time_local_15min = cte_telemetry.date_time_local_15min)
 AND (cte_telemetry.asset_id = cte_assets.asset_id)
-- Ensure we get at least 7 days of data, but the min(date) leaves this the option to use an extended time range in the future
WHERE dh.date >= (SELECT LEAST(IFNULL(MIN(date_local),DATEADD(DAY,-7,CURRENT_DATE)),DATEADD(DAY,-7,CURRENT_DATE)) FROM cte_telemetry)
  AND dh.date <= (SELECT MAX(date_local) FROM cte_telemetry)
GROUP BY 
    dh.date,
    dh.date_time_local_hour,
    dh.date_time_local_15min,
    cte_assets.asset_id,
    cte_assets.asset_name,
    cte_assets.model_id_asset,
    cte_assets.site_id,
    cte_assets.site_name,
    cte_assets.capability_models
;