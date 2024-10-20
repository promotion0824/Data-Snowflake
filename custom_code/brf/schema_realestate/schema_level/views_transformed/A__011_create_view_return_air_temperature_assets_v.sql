-- ******************************************************************************************************************************
-- Create view for Comfort dashboard
-- ReturnAirTemperatureSensor and ReturnAirTemperatureSetpoint
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.return_air_temperature_assets_v AS
-- First get all sites besides Grace Building
WITH cte_temperature_sensors AS (
	SELECT DISTINCT
        ca.asset_id,
		ca.model_id_asset,
		ca.asset_name,
		a.display_name_en AS category_name,
		ca.asset_detail:customProperties.excludedFromComfortAnalytics::STRING AS excludedFromComfortAnalytics,
		COALESCE(a.floor_id, sp.floor_id) AS floor_id,
		ca.trend_id,
		ca.unit,
        ca.model_id,
		ca.building_id,
		ca.building_name,
        ca.site_id,
		ca.site_name,
		ca.time_zone
		FROM transformed.capabilities_assets ca
		JOIN transformed.return_air_handling_unit_assets a 
		  ON (ca.asset_id = a.asset_id)
		LEFT JOIN transformed.assets_space sp
          ON (ca.asset_id = sp.asset_id)
		 AND (sp.model_id_space = 'dtmi:com:willowinc:Level;1')
		WHERE
            ca.model_id IN ('dtmi:com:willowinc:ReturnAirTemperatureSensor;1','dtmi:com:willowinc:ReturnAirHumiditySensor;1')
        AND ca.site_id != '24695d9d-269c-4763-966c-b3ab5992dc52' 
		AND ca.unit IN ('%RH', 'degF') 
)
,cte_temperature_setpoints AS (
	SELECT
		ca.asset_id,
		ca.model_id_asset,
		ca.asset_name,
		a.display_name_en AS category_name,
		a.floor_id,
		ca.trend_id,
		ca.unit,
		ca.model_id,
		ca.site_id
		FROM transformed.capabilities_assets ca
		JOIN transformed.return_air_handling_unit_assets a 
		  ON (ca.asset_id = a.asset_id)
		WHERE
            ca.model_id = 'dtmi:com:willowinc:ReturnAirTemperatureSetpoint;1'
		AND ca.site_id != '24695d9d-269c-4763-966c-b3ab5992dc52' 
        AND ca.unit IN ('degF') 
)

-- union the Grace Building
,cte_temperature_sensors_grace AS (
	SELECT
            ca.asset_id,
    		ca.model_id_asset,
    		ca.asset_name,
    		a.display_name_en AS category_name,
    		ca.asset_detail:customProperties.excludedFromComfortAnalytics::STRING AS excludedFromComfortAnalytics,
    		COALESCE(a.floor_id, sp.floor_id) AS floor_id,
    		ca.trend_id,
    		ca.unit,
            ca.model_id,
			ca.building_id,
			ca.building_name, 
            ca.site_id,
    		ca.site_name,
    		ca.time_zone
		FROM transformed.capabilities_assets ca
		JOIN transformed.return_air_handling_unit_assets a 
		  ON (ca.asset_id = a.asset_id)
		LEFT JOIN transformed.assets_space sp
          ON (ca.asset_id = sp.asset_id)
		 AND (sp.model_id_space = 'dtmi:com:willowinc:Level;1')
		WHERE
            ca.model_id_asset = 'dtmi:com:willowinc:HVACEquipmentGroup;1'
        AND ca.model_id IN ('dtmi:com:willowinc:ReturnAirTemperatureSensor;1')
        AND ca.unit IN ('%RH', 'degF') 
        AND ca.site_id = '24695d9d-269c-4763-966c-b3ab5992dc52' 
        
)
,cte_temperature_setpoints_grace AS (
	SELECT
		ca.asset_id,
		ca.model_id_asset,
		ca.asset_name,
		a.display_name_en AS category_name,
		a.floor_id,
		ca.trend_id,
		ca.unit,
		ca.model_id,
		ca.site_id
		FROM transformed.capabilities_assets ca
		JOIN transformed.return_air_handling_unit_assets a 
		  ON (ca.asset_id = a.asset_id)
		WHERE
            ca.model_id IN ('dtmi:com:willowinc:ZoneAirTemperatureSetpoint;1','dtmi:com:willowinc:TargetZoneAirTemperatureSetpoint;1')
        AND ca.unit IN ('degF')
        AND ca.site_id = '24695d9d-269c-4763-966c-b3ab5992dc52' 
)
    SELECT 
        sensors.asset_id,
		sensors.model_id_asset,
		sensors.asset_name,
		sensors.category_name,
		sensors.excludedFromComfortAnalytics,
		sensors.trend_id AS sensor_trend_id,
        setpoints.trend_id AS setpoint_trend_id,
		sensors.unit,
		sensors.floor_id,
        sensors.model_id,
		sensors.building_id,
		sensors.building_name,
        sensors.site_id,
		sensors.site_name,
		sensors.time_zone
	FROM cte_temperature_sensors sensors
		LEFT JOIN cte_temperature_setpoints setpoints
			ON (sensors.asset_id = setpoints.asset_id)
			AND (sensors.unit = setpoints.unit)

UNION ALL

    SELECT 
        sensors.asset_id,
		sensors.model_id_asset,
		sensors.asset_name,
		sensors.category_name,
		sensors.excludedFromComfortAnalytics,
		sensors.trend_id AS sensor_trend_id,
        setpoints.trend_id AS setpoint_trend_id,
		sensors.unit,
		sensors.floor_id,
        sensors.model_id,
		sensors.building_id,
		sensors.building_name,
        sensors.site_id,
		sensors.site_name,
		sensors.time_zone
	FROM cte_temperature_sensors_grace sensors
		LEFT JOIN cte_temperature_setpoints_grace setpoints
			ON (sensors.asset_id = setpoints.asset_id)
			AND (sensors.unit = setpoints.unit)
	;

CREATE OR REPLACE TRANSIENT TABLE transformed.return_air_temperature_assets AS SELECT * FROM transformed.return_air_temperature_assets_v;
