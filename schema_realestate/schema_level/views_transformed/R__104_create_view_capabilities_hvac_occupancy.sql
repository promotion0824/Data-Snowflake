-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.capabilities_hvac_occupancy_v AS
	SELECT 
		ca.capability_name,
		ca.trend_id,
		ca.model_id AS model_id,
		ca.capability_type,
		ca.site_id,
		ca.id AS id,
		ca.tags AS capablity_tags,
		he.id AS equipment_id,
		he.equipment_name,
		he.model_id AS equipment_model_id,
		he.tags AS equipment_tags,
		he.space_id,
		he.space_detail:customProperties.name::string AS space_name,
		split_part(REPLACE(he.space_detail:modelId::string, ';',':'),':',4) AS space_type,
		s.space_capacity AS capacity,
		s.usable_area_space AS usable_area,
		l.level_name,
		l.id AS level_id,
		l.building_id,
		he.space_detail,
		l.building_detail
	FROM transformed.capabilities_assets ca
		JOIN transformed.hvac_equipment he 
			ON (ca.asset_id = he.id)
		LEFT JOIN transformed.spaces_levels s 
			ON (he.space_id = s.id)
		LEFT JOIN transformed.levels_buildings l 
			ON (s.level_id = l.id)
	WHERE ca.model_id IN (
		'dtmi:com:willowinc:AirFlowSensor;1',
		'dtmi:com:willowinc:ZoneAirTemperatureSensor;1',
		'dtmi:com:willowinc:AirTemperatureSensor;1',
		'dtmi:com:willowinc:DamperStatusSensor;1',
		'dtmi:com:willowinc:DischargeAirTemperatureSensor;1',
		'dtmi:com:willowinc:StatusSensor;1',
		'dtmi:com:willowinc:AngularVelocitySensor;1',
		'dtmi:com:willowinc:OutsideAirTemperatureSensor;1',
		'dtmi:com:willowinc:AirCO2Sensor;1',
		'dtmi:com:willowinc:AirCO2Setpoint;1',
		'dtmi:com:willowinc:ReturnAirTemperatureSensor;1',
		'dtmi:com:willowinc:DischargeAirFlowSensor;1',
		'dtmi:com:willowinc:FanSpeedActuator;1'
  )
	UNION ALL
	SELECT 
		capability_name,
		trend_id,
		model_id_capability AS model_id,
		capability_type,
		site_id,
		sensor_id AS capability_id,
		NULL AS capablity_tags,
		NULL AS equipment_tags,
		NULL AS equipment_id,
		NULL AS equipment_name,
		NULL AS equipment_model_id,
		space_id,
		space_name,
		space_type,
		capacity,
		usable_area,
		level_name,
		level_id,
		NULL AS building_id,
		NULL AS space_detail,
		NULL AS building_detail
	FROM transformed.occupancy
	WHERE capability_name IN (
		'Net People Count'
);

CREATE OR REPLACE TABLE transformed.capabilities_hvac_occupancy AS SELECT * FROM transformed.capabilities_hvac_occupancy_v;