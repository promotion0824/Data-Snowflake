-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************


CREATE OR REPLACE VIEW published.fan_coil_unit_measurements AS

    WITH cte_assets AS (
        SELECT DISTINCT
            asset_id,
            model_id_asset,
            asset_name,
            site_id,
            site_name,
            space_name,
            space_type,
            level_name,
            floor_sort_order
        FROM transformed.fan_coil_unit_assets
        )
        SELECT 
            a.asset_id,
            a.model_id_asset,
            a.asset_name,
            m.date_local,
            m.date_time_local_15min,
            m.mode_sensor,
            m.zone_air_temperature,
            m.sample_count,
            a.site_id,
            a.site_name,
            a.space_name,
            a.space_type,
            a.level_name,
            a.floor_sort_order
        FROM transformed.fan_coil_unit_measurements m
        JOIN cte_assets a on m.asset_id = a.asset_id;