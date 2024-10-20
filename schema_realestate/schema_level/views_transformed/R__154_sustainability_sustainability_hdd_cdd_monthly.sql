-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW transformed.sustainability_hdd_cdd_monthly AS
WITH cte_degreedays AS (
    SELECT  DISTINCT
        DATE_TRUNC('MONTH',ts.date_local) AS month_start_date,
        LEFT(ts.date_local, 7) AS month,
        CASE WHEN ca.model_id IN ('dtmi:com:willowinc:HeatingDegreeDays;1') THEN MIN(telemetry_value) OVER (PARTITION BY month, ca.building_id, ca.model_id) ELSE NULL END AS hdd_min,
        CASE WHEN ca.model_id IN ('dtmi:com:willowinc:HeatingDegreeDays;1') THEN MAX(telemetry_value) OVER (PARTITION BY month, ca.building_id, ca.model_id) ELSE NULL END AS hdd_max,
        CASE WHEN ca.model_id IN ('dtmi:com:willowinc:CoolingDegreeDays;1') THEN MIN(telemetry_value) OVER (PARTITION BY month, ca.building_id, ca.model_id) ELSE NULL END AS cdd_min,
        CASE WHEN ca.model_id IN ('dtmi:com:willowinc:CoolingDegreeDays;1') THEN MAX(telemetry_value) OVER (PARTITION BY month, ca.building_id, ca.model_id) ELSE NULL END AS cdd_max,
        ca.unit AS temperature_unit,
        ca.building_id,
        ca.building_name
    FROM transformed.capabilities_assets ca
    JOIN transformed.telemetry ts ON ca.external_id = ts.external_id
    WHERE 
        ts.date_local > '2024-03-22'
    AND model_id IN ('dtmi:com:willowinc:HeatingDegreeDays;1','dtmi:com:willowinc:CoolingDegreeDays;1') 
)
SELECT
month_start_date,
building_id,
temperature_unit,
SUM(hdd_max - hdd_min) AS hdd, 
SUM(cdd_max - cdd_min) AS cdd,
hdd + cdd as degree_days
FROM cte_degreedays
GROUP BY
month_start_date,
building_id,
temperature_unit
;