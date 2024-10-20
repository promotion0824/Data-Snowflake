-- ------------------------------------------------------------------------------------------------------------------------------
--  Create View to expoert the sites data for Degree Days
--  COPY INTO @raw.ADHOC_ESG/site_core_sites/ FROM transformed.sites_long_lat file_format = (type = 'JSON', COMPRESSION = GZIP) OVERWRITE = TRUE;
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW transformed.sites_long_lat AS
WITH cte_region AS (
    SELECT 'aue2' AS server_region, 'celsius' AS temperature_unit, 15.5 AS temperature_threshold
    UNION ALL
    SELECT 'eu22' AS server_region, 'fahrenheit' AS temperature_unit, 65 AS temperature_threshold
    UNION ALL
    SELECT 'weu2' AS server_region, 'celsius' AS temperature_unit, 15.5 AS temperature_threshold
)
SELECT
    OBJECT_CONSTRUCT(
        'customer_id', customer_id, 
        'site_id', id,
        'site_name', name, 
        'latitude', latitude,
        'longitude', longitude,
        'region', server_region,
        'temperature_unit', CASE WHEN id = 'a226929d-6e27-480f-b8dd-40ffbc47024c' THEN 'fahrenheit' ELSE temperature_unit END,
        'temperature_threshold', CASE WHEN id = 'a226929d-6e27-480f-b8dd-40ffbc47024c' THEN 65 ELSE temperature_threshold END
    ) AS raw_json_value
FROM transformed.site_core_sites s
JOIN cte_region ON cte_region.server_region = SPLIT_PART(s.server,'-', 4)
WHERE NOT name ILIKE ANY ('%delete%','%test site%', '%new%test%')
;

