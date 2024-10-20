-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW published.sustainability_resources_normalization AS 
WITH cte_cbecs AS (
    SELECT score, mercantile AS cbecs_value, LEAD(mercantile,1) OVER (ORDER BY score desc) AS next_value
    FROM transformed.sustainability_cbecs_score 
    UNION ALL
    SELECT 100, 0 AS cbecs_value, MIN(mercantile) AS next_value 
    FROM transformed.sustainability_cbecs_score 
    UNION ALL
    SELECT 0, MAX(mercantile) AS cbecs_value, 999999 AS next_value 
    FROM transformed.sustainability_cbecs_score 
)

,cte_eui AS (
SELECT DISTINCT
    month_start_date,
    building_id,
    building_name,
    property_type,
    property_year_built,
    resource_type,
    data_type,
    energy_consumption * 1000 AS energy_used,
    'kBTU' AS energy_unit, 
    building_gross_area,
    CASE WHEN resource_type ILIKE '%Water%' THEN conformed_unit || '/' || REPLACE(building_gross_area_unit,'squareFoot','sf') ELSE 'kBTU/'|| REPLACE(building_gross_area_unit,'squareFoot','sf') END AS normalization_unit,
    CASE WHEN resource_type ILIKE '%Water%' THEN amount / building_gross_area ELSE energy_used / building_gross_area END AS normalization_value,
    '$/'|| REPLACE(building_gross_area_unit,'squareFoot','sf') AS normalized_cost_unit,
    cost / building_gross_area AS normalized_cost,
    'by Area' AS normalization_type
FROM transformed.sustainability_resources_comparison
WHERE resource_type NOT IN ('Sewage')
)
,cte_eui_sum AS (
SELECT DISTINCT
    building_id,
    building_name,
    property_type,
    property_year_built,
    'Energy' AS resource_type,
    SUM(energy_consumption) * 1000 AS energy_used,
    'kBTU' AS energy_unit, 
    CASE WHEN data_type = 'Actual' THEN energy_used ELSE 0 END AS energy_actual,
    CASE WHEN data_type = 'Metered' THEN energy_used ELSE 0 END AS energy_metered,
    building_gross_area,
    'kBTU/'|| REPLACE(MAX(building_gross_area_unit),'squareFoot','sf') AS normalization_unit,
    GREATEST(energy_actual,energy_metered) / building_gross_area AS normalization_value,
    CASE WHEN energy_metered > energy_actual THEN 'Metered' ELSE 'Actual' END AS data_type,
    '$/'|| REPLACE(MAX(building_gross_area_unit),'squareFoot','sf') AS normalized_cost_unit,
    SUM(cost) / MAX(building_gross_area) AS normalized_cost
FROM transformed.sustainability_resources_comparison
WHERE resource_type in ('Electricity','Natural Gas')
AND month_start_date >= DATEADD('month',-13,SYSDATE())
GROUP BY building_id, building_name, property_type, property_year_built, building_gross_area,data_type
)

SELECT * FROM cte_eui

UNION ALL
SELECT DISTINCT
    rc.month_start_date,
    rc.building_id,
    rc.building_name,
    rc.property_type,
    rc.property_year_built,
    rc.resource_type,
    rc.data_type,
    rc.energy_used,
    rc.energy_unit,  
    rc.building_gross_area,
    'kBTU/sf-'|| deg.temperature_unit || '-day' AS normalization_unit,
    energy_used / deg.degree_days AS normalization_value,
    normalized_cost_unit || '-' || deg.temperature_unit || '-day',
    normalized_cost,
    'by Weather' AS normalization_type
FROM cte_eui rc
LEFT JOIN transformed.sustainability_hdd_cdd_monthly deg on rc.month_start_date = deg.month_start_date and rc.building_id = deg.building_id
WHERE resource_type in ('Electricity','Natural Gas','Steam')

UNION ALL
SELECT DISTINCT
    rc.month_start_date,
    rc.building_id,
    rc.building_name,
    rc.property_type,
    rc.property_year_built,
    rc.resource_type,
    rc.data_type,
    rc.energy_consumption * 1000 AS energy_used, 
    rc.energy_unit,
    rc.building_gross_area,
    NULL AS normalization_unit,
    NULL AS normalization_value,
    NULL AS normalized_cost_unit,
    NULL AS normalized_cost,
    'by Energy Star' AS normalization_type
FROM transformed.sustainability_resources_comparison rc
WHERE resource_type in ('Electricity','Natural Gas','Steam')

UNION ALL

SELECT
    NULL AS month_start_date,
    building_id,
    building_name,
    property_type,
    property_year_built,
    'Energy' AS resource_type,
    data_type,
    energy_used,
    energy_unit,
    building_gross_area,
    'Percentile' AS normalization_unit,
    cbecs.score AS normalization_value,
    normalized_cost_unit,
    normalized_cost,
    'by CBECS Score' AS normalization_type
FROM cte_eui_sum eui_sum
JOIN cte_cbecs cbecs ON eui_sum.normalization_value BETWEEN cbecs.cbecs_value AND cbecs.next_value
;