-- ------------------------------------------------------------------------------------------------------------------------------
-- Create View 
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.sites_weather_data AS
WITH weather_data AS (
    SELECT DISTINCT 
        ts.date_local AS date,
        LEFT(SPLIT_PART(TIME_SLICE(ts.timestamp_local, 15, 'MINUTE'),' ',2),5) AS time_local_15min,
        d.is_weekday,
        CASE WHEN ca.model_id IN ('dtmi:com:willowinc:HeatingDegreeDays;1') THEN MAX(telemetry_value) ELSE NULL END AS hdd,
        CASE WHEN ca.model_id IN ('dtmi:com:willowinc:CoolingDegreeDays;1') THEN MAX(telemetry_value) ELSE NULL END AS cdd,
        ca.unit AS temperature_unit,
        ca.model_id,
        ca.site_id,
        ca.site_name,
        ca.building_id,
        ca.building_name
    FROM transformed.capabilities_assets ca
    JOIN transformed.telemetry ts ON ca.external_id = ts.external_id
    JOIN transformed.dates d ON ts.date_local = d.date
    WHERE 
        ts.date_local > '2024-01-01'
    AND model_id IN ('dtmi:com:willowinc:HeatingDegreeDays;1','dtmi:com:willowinc:CoolingDegreeDays;1') 
    GROUP BY
        ts.date_local,
        LEFT(SPLIT_PART(TIME_SLICE(ts.timestamp_local, 15, 'MINUTE'),' ',2),5),
        d.is_weekday,
        ca.unit,
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
    site_id,
    site_name,
    building_id,
    building_name
FROM weather_data
GROUP BY
    date,
    is_weekday,
    temperature_unit,
    site_id,
    site_name,
    building_id,
    building_name
-- UNION ALL
-- SELECT DISTINCT
--     d.date,
--     d.is_weekday,
--     dd.hdd,
--     dd.cdd,
--     dd.temperature_unit,
--     ca.site_id,
--     ca.site_name,
--     ca.building_id,
--     ca.building_name
-- FROM transformed.sites_weather_historical dd
-- LEFT JOIN transformed.capabilities_assets ca
-- JOIN transformed.dates d ON  d.date = dd.date
-- WHERE dd.date <= '2024-01-21'
;