-- ------------------------------------------------------------------------------------------------------------------------------
-- Create View - sourced from weatherBit
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW transformed.sites_weather_data_v AS
WITH weather_data AS (
    SELECT DISTINCT 
        ts.date_local AS date,
        LEFT(SPLIT_PART(TIME_SLICE(ts.timestamp_local, 15, 'MINUTE'),' ',2),5) AS time_local_15min,
        d.is_weekday,
        CASE WHEN ca.model_id IN ('dtmi:com:willowinc:HeatingDegreeDays;1') THEN MAX(telemetry_value) ELSE NULL END AS hdd,
        CASE WHEN ca.model_id IN ('dtmi:com:willowinc:CoolingDegreeDays;1') THEN MAX(telemetry_value) ELSE NULL END AS cdd,
        ca.unit AS temperature_unit,
        ca.asset_id,
        ca.model_id,
        ca.site_id,
        ca.site_name,
        ca.building_id,
        ca.building_name
    FROM transformed.capabilities_assets ca
    JOIN transformed.telemetry ts ON ca.external_id = ts.external_id
    JOIN transformed.dates d ON ts.date_local = d.date
    WHERE 
        ts.date_local > '2024-05-06'
    AND model_id IN ('dtmi:com:willowinc:HeatingDegreeDays;1','dtmi:com:willowinc:CoolingDegreeDays;1') 
    GROUP BY
        ts.date_local,
        LEFT(SPLIT_PART(TIME_SLICE(ts.timestamp_local, 15, 'MINUTE'),' ',2),5),
        d.is_weekday,
        ca.unit,
        ca.asset_id,
        ca.model_id,
        ca.site_id,
        ca.site_name,
        ca.building_id,
        ca.building_name
)
SELECT 
    date,
    is_weekday,
    ROUND(max(hdd) - IFNULL(min(hdd),0),2) AS hdd,
    ROUND(max(cdd) - IFNULL(min(cdd),0),2) AS cdd,
    REPLACE(temperature_unit,'-Day','') AS temperature_unit,
    COALESCE(b.site_id,w.site_id) AS site_id,
    COALESCE(b.site_name,w.site_name) AS site_name,
    COALESCE(b.building_id,w.building_id) AS building_id,
    COALESCE(b.building_name,w.building_name) AS building_name
FROM weather_data w
LEFT JOIN transformed.twins_relationships_deduped tr 
  ON w.asset_id = tr.target_twin_id
 AND tr.relationship_name = 'servedBy'
LEFT JOIN transformed.buildings b
  ON tr.source_twin_id = b.building_id
WHERE IFNULL(tr.is_deleted,false) = false
GROUP BY
    date,
    is_weekday,
    temperature_unit,
    COALESCE(b.site_id,w.site_id),
    COALESCE(b.site_name,w.site_name),
    COALESCE(b.building_id,w.building_id),
    COALESCE(b.building_name,w.building_name)
UNION ALL
SELECT DISTINCT
    d.date,
    d.is_weekday,
    dd.hdd,
    dd.cdd,
    'degF'  AS temperature_unit,
    ca.site_id,
    ca.site_name,
    ca.building_id,
    ca.building_name
FROM transformed.nau_degreedays dd
LEFT JOIN transformed.capabilities_assets ca
JOIN transformed.dates d ON  d.date = dd.date
WHERE dd.date <= '2024-05-06'
;

CREATE OR REPLACE TABLE transformed.sites_weather_data AS SELECT * FROM transformed.sites_weather_data_v;