-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW transformed.hvac_adjusted_capabilities_v AS
	SELECT
	c.model_name_asset,
	c.model_name_capability,
	c.tags_string,
	CASE  
		WHEN model_name_asset = 'CAVBox' AND c.tags_string ILIKE '%outside%' THEN 'Outside Air Flow'
		WHEN model_name_capability = 'OutsideAirFlowSensor' AND c.tags_string ILIKE '%outside%' AND c.tags_string NOT ILIKE '%control%' THEN 'Unit Outside Air Flow'
		WHEN model_name_capability = 'ZoneAirTemperatureSensor' AND o.model_level_4 ILIKE '%TerminalUnit%' THEN 'Actual Zone Temperature'
		WHEN model_name_capability = 'ReturnAirTemperatureSensor' AND c.tags_string ILIKE '%air%return%sensor%temp%' THEN 'Return Air Temperature'
		WHEN model_name_capability = 'AirCO2Sensor' AND c.tags_string ILIKE '%outside%' THEN 'Outside CO2'
		WHEN model_name_capability = 'AirCO2Sensor' AND c.tags_string ILIKE '%return%' THEN 'Return Air CO2'
		WHEN model_name_capability = 'AirPressureSensor' AND c.tags_string ILIKE '%discharge%' AND c.tags_string NOT ILIKE '%sp%' AND c.tags_string NOT ILIKE '%alarm%' THEN 'Supply Air Static Pressure Control'
		WHEN model_name_capability = 'DischargeAirTemperatureSensor' AND c.tags_string NOT ILIKE '%sp%' AND c.tags_string NOT ILIKE '%alarm%' THEN 'Supply Air Temperature'
		WHEN model_name_capability = 'CO2AirQualitySensor' AND c.tags_string NOT ILIKE '%sp%' AND model_id_asset in ('dtmi:com:willowinc:SensorEquipment;1','dtmi:com:willowinc:FanCoilUnit;1') THEN 'Zone CO2'
	ELSE null
	END AS adjusted_capability_name,
	c.capability_name,
	c.id AS capability_id,
	c.model_id_capability,
	c.unique_id,
	c.trend_id,
	c.trend_interval,
	c.unit,
	c.enabled,
	c.capability_type,
	c.description,
	c.tags AS tags_capability,
	c.asset_id,
	c.asset_name,
	c.asset_detail[0].tags::STRING AS tags_asset,
	c.model_id_asset,
	c.asset_detail,
	c.space_id,
	c.space_name,
	c.space_type,
	c.capacity,
	c.usable_area,
	c.level_name,
	c.level_id,
	c.level_code,
	c.building_id,
	c.site_id,
	c.space_detail,
	c.building_detail,
	o.model_level_4 AS ontology_model_level_4,
	o.model_level_5 AS ontology_model_level_5,
	o.model_level_6 AS ontology_model_level_6
	FROM transformed.capabilities_assets_details c 
		LEFT JOIN transformed.ontology_buildings o 
			ON (c.model_id_asset = o.id)
;

CREATE OR REPLACE TABLE transformed.hvac_adjusted_capabilities AS SELECT * FROM transformed.hvac_adjusted_capabilities_v;