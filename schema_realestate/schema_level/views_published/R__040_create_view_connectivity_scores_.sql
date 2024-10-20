-- ------------------------------------------------------------------------------------------------------------------------------
-- Create view
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.connectivity_scores AS
SELECT
    c.date,
    date_time_local_15min,
    date_time_local_hour,
    IFF(HOUR(date_time_local_hour) >= COALESCE(working_hours.default_value:hourStart,8) 
    AND HOUR(date_time_local_hour) <  COALESCE(working_hours.default_value:hourEnd, 18), true, false) AS is_working_hour,
    d.is_weekday,
    'Energy' AS asset_type,
    asset_id,
    asset_name,
    building_id,
    building_name,
    c.site_id,
    site_name,
    SUM(trend_count_expected) AS expected_readings,
    SUM(trend_count_actual) AS actual_readings,
    ROUND(IFNULL(actual_readings,0)/expected_readings * 100,2) AS connectivity_score,
    model_id_asset,
    capability_models,
    MIN(num_sensors) AS num_sensors
FROM transformed.connectivity_energy_materialized c
JOIN transformed.dates d ON c.date = d.date
LEFT JOIN transformed.site_defaults working_hours
     ON (c.site_id = working_hours.site_id AND working_hours.type = 'WorkingHours')
    AND (working_hours._valid_from <= c.date_time_local_hour AND working_hours._valid_to >= c.date_time_local_hour)
WHERE asset_type = 'Energy'
GROUP BY 
    c.date,
    date_time_local_15min,
    date_time_local_hour,
    d.is_weekday,
    is_working_hour,
    asset_id,
    asset_name,
    building_id,
    building_name,
    c.site_id,
    site_name,
    model_id_asset,
    capability_models
    
-- UNION ALL

-- SELECT
--     date,
--     date_time_local_15min,
--     date_time_local_hour,
--     'Comfort' AS asset_type,
--     asset_id,
--     asset_name,
--     site_id,
--     site_name,
--     SUM(trend_count_expected) AS expected_readings,
--     SUM(trend_count_actual) AS actual_readings,
--     ROUND(IFNULL(actual_readings,0)/expected_readings * 100,2) AS connectivity_score,
--     model_id_asset,
--     capability_models,
--     MIN(num_sensors) AS num_sensors
-- FROM transformed.connectivity_comfort_detail
-- WHERE asset_type = 'Comfort'
-- GROUP BY
--     date,
--     date_time_local_15min,
--     date_time_local_hour,
--     asset_id,
--     asset_name,
--     site_id,
--     site_name,
--     model_id_asset,
--     capability_models
;