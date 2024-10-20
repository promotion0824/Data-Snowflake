-- ******************************************************************************************************************************
-- Create view for Comfort dashboard
-- AirTemperatureSensor and AirTemperatureSetpoint
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.air_temperature_assets_v AS
WITH cte_temperature_sensors AS (
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
		JOIN transformed.air_handling_unit_assets a 
		  ON (ca.asset_id = a.asset_id)
		WHERE
            ca.model_id = 'dtmi:com:willowinc:DischargeAirTemperatureSensor;1' 
        AND ca.unit IN ('degC', 'degF') 
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
		JOIN transformed.air_handling_unit_assets a 
		  ON (ca.asset_id = a.asset_id)
		WHERE
            ca.model_id = 'dtmi:com:willowinc:AirTemperatureSetpoint;1' 
        AND ca.unit IN ('degC', 'degF') 
)
    SELECT 
    
        sensors.asset_id,
		sensors.model_id_asset,
		sensors.asset_name,
		sensors.category_name,
		sensors.trend_id AS sensor_trend_id,
        setpoints.trend_id AS setpoint_trend_id,
		sensors.unit,
		sensors.floor_id,
        sensors.model_id,
        sensors.site_id
FROM cte_temperature_sensors sensors
    LEFT JOIN cte_temperature_setpoints setpoints
           ON (sensors.asset_id = setpoints.asset_id)
          AND (sensors.unit = setpoints.unit)
;

CREATE OR REPLACE TRANSIENT TABLE transformed.air_temperature_assets AS SELECT * FROM transformed.air_temperature_assets_v;