-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW transformed.comfort_assets_v AS
WITH cte_temperature_sensors AS (
	SELECT DISTINCT
                t.model_id_asset,
                t.asset_id,
                t.asset_name,
                t.excludedFromComfortAnalytics,
                t.sensor_type,
                t.trend_id,
                t.unit,
                t.building_id,
                t.building_name,
                t.site_id,
                t.time_zone,
                IFNULL(t.zone_id,'') AS zone_id,
                IFNULL(t.zone_name,'') AS zone_name,
                IFNULL(t.room_id, '') AS room_id,
                IFNULL(t.room_name, '') AS room_name,
                t.level_name,
                t.floor_sort_order
        FROM transformed.comfort_twins t
	WHERE
              t.unit IN ('degC', 'degF') 
          AND t.sensor_type = 'temperature_sensor' 
)
,cte_temperature_setpoints AS (
	SELECT DISTINCT
                t.model_id_asset,
                t.asset_id,
                t.asset_name,
                t.excludedFromComfortAnalytics,
                t.sensor_type,
                t.capability_model,
                t.trend_id,
                t.unit,
                t.building_id,
                t.building_name,
                t.site_id,
                t.time_zone,
                IFNULL(t.zone_id,'') AS zone_id,
                IFNULL(t.zone_name,'') AS zone_name,
                IFNULL(t.room_id, '') AS room_id,
                IFNULL(t.room_name, '') AS room_name,
                t.level_name,
                t.floor_sort_order
        FROM transformed.comfort_twins t
	WHERE 
              t.unit IN ('degC', 'degF') 
          AND t.sensor_type = 'setpoint_sensor'
)
,cte_setpoint_offset AS (
	SELECT DISTINCT
                t.model_id_asset,
                t.asset_id,
                t.asset_name,
                t.excludedFromComfortAnalytics,
                t.sensor_type,
                t.capability_model,
                t.trend_id,
                t.unit,
                t.building_id,
                t.building_name,
                t.site_id,
                t.time_zone,
                IFNULL(t.zone_id,'') AS zone_id,
                IFNULL(t.zone_name,'') AS zone_name,
                IFNULL(t.room_id, '') AS room_id,
                IFNULL(t.room_name, '') AS room_name,
                t.level_name,
                t.floor_sort_order
        FROM transformed.comfort_twins t
	WHERE 
              t.unit IN ('degC', 'degF') 
          AND t.sensor_type = 'setpoint_offset'
)
    SELECT 
                sensors.model_id_asset,
                sensors.asset_id,
                sensors.asset_name,
                sensors.excludedFromComfortAnalytics,
                sensors.trend_id AS sensor_trend_id,
                setpoints.trend_id AS setpoint_trend_id,
                setpoints.capability_model AS setpoint_model_id,
                offsets.trend_id AS offset_trend_id,
                offsets.capability_model AS offset_model_id,
                sensors.unit,
                sensors.building_id,
                sensors.building_name,
                sensors.site_id,
                sensors.time_zone,
                sensors.zone_id, 
                sensors.zone_name, --hvac zone
                sensors.room_id,
                sensors.room_name,
                sensors.level_name,
                sensors.floor_sort_order,
                SYSDATE() AS last_updated
FROM cte_temperature_sensors sensors
    LEFT JOIN cte_temperature_setpoints setpoints
           ON (sensors.asset_id = setpoints.asset_id)
          AND (sensors.unit = setpoints.unit)
          AND (sensors.zone_id = setpoints.zone_id)
          AND (sensors.room_id = setpoints.room_id)
    LEFT JOIN cte_setpoint_offset offsets
           ON (sensors.asset_id = offsets.asset_id)
          AND (sensors.unit = offsets.unit)
          AND (sensors.zone_id = offsets.zone_id)
          AND (sensors.room_id = offsets.room_id)
;
CREATE OR REPLACE TABLE transformed.comfort_assets AS SELECT * FROM transformed.comfort_assets_v;
