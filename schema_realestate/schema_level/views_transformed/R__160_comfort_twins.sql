-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.comfort_twins AS
    SELECT DISTINCT
            ca.model_id as capability_model, 
            CASE WHEN ca.model_id IN ('dtmi:com:willowinc:ZoneAirTemperatureSensor;1') 
                    THEN 'temperature_sensor' 
                WHEN ca.model_id IN (
                    'dtmi:com:willowinc:AirTemperatureSetpoint;1',
                    'dtmi:com:willowinc:CoolingZoneAirTemperatureSetpoint;1',
                    'dtmi:com:willowinc:HeatingZoneAirTemperatureSetpoint;1',
                    'dtmi:com:willowinc:EffectiveCoolingZoneAirTemperatureSetpoint;1',
                    'dtmi:com:willowinc:EffectiveHeatingZoneAirTemperatureSetpoint;1',
                    'dtmi:com:willowinc:EffectiveZoneAirTemperatureSetpoint;1',
                    'dtmi:com:willowinc:TargetZoneAirTemperatureSetpoint;1',
                    'dtmi:com:willowinc:OccupancySensor;1',
                    'dtmi:com:willowinc:OccupiedState;1',
                    'dtmi:com:willowinc:OccupancySetpoint;1',
                    'dtmi:com:willowinc:OccupiedActuator;1',
                    'dtmi:com:willowinc:OccupiedCoolingSetpoint;1',
                    'dtmi:com:willowinc:OccupiedCoolingZoneAirTemperatureSetpoint;1',
                    'dtmi:com:willowinc:OccupiedHeatingSetpoint;1',
                    'dtmi:com:willowinc:OccupiedHeatingZoneAirTemperatureSetpoint;1',
                    'dtmi:com:willowinc:UnoccupiedCoolingSetpoint;1',
                    'dtmi:com:willowinc:UnoccupiedCoolingZoneAirTemperatureSetpoint;1',
                    'dtmi:com:willowinc:UnoccupiedHeatingSetpoint;1',
                    'dtmi:com:willowinc:UnoccupiedHeatingZoneAirTemperatureSetpoint;1',
                    'dtmi:com:willowinc:ZoneAirTemperatureSetpoint;1',
                    'dtmi:com:willowinc:TargetZoneAirTemperatureSetpoint;1'
                    )
                    THEN 'setpoint_sensor'
                WHEN ca.model_id IN (
                    'dtmi:com:willowinc:SetpointOffset;1',
                    'dtmi:com:willowinc:Offset;1'
                    ) 
                    THEN 'setpoint_offset'
                ELSE 'unknown' 
            END AS sensor_type, 
            ca.capability_id,
            ca.capability_name,
            ca.trend_id,
            ca.external_id,
            ca.unit,
            ca.building_id,
            ca.building_name,
            ca.site_id,
            ca.time_zone,
            ca.model_id_asset,
            ca.asset_id, 
            ca.asset_name,
            t.twin_id AS zone_id,
            COALESCE(t.name,'none') AS zone_name,
            COALESCE(t4.twin_id,t3.twin_id) AS room_id,
            COALESCE(t4.name, t3.name) AS room_name, 
            COALESCE(lb2.level_name,lb.level_name,t3.name) AS level_name,
            COALESCE(lb2.floor_sort_order,lb.floor_sort_order) AS floor_sort_order,
            ca.asset_detail:customProperties.excludedFromComfortAnalytics::STRING AS excludedFromComfortAnalytics
    FROM transformed.capabilities_assets ca
    JOIN transformed.ontology_models o ON ca.model_id_asset = o.id
    LEFT JOIN transformed.twins_relationships_deduped tr ON ca.asset_id = tr.target_twin_id AND tr.relationship_name = 'isFedBy'
    LEFT JOIN transformed.twins t ON tr.source_twin_id = t.twin_id AND t.model_id IN ('dtmi:com:willowinc:HVACZone;1')
    LEFT JOIN transformed.twins_relationships_deduped tr2 ON t.twin_id = tr2.target_twin_id AND tr2.relationship_name = 'isPartOf'
    LEFT JOIN transformed.twins t3 ON tr2.source_twin_id = t3.twin_id AND t3.model_id IN ('dtmi:com:willowinc:Room;1','dtmi:com:willowinc:Level;1')
    LEFT JOIN transformed.twins_relationships_deduped tr3 ON t3.twin_id = tr3.source_twin_id AND tr3.relationship_name = 'isPartOf'
    LEFT JOIN transformed.twins_relationships_deduped tr4 ON ca.asset_id = tr4.source_twin_id and tr4.relationship_name IN ('locatedIn','isPartOf')
    LEFT JOIN transformed.twins t4 ON tr4.target_twin_Id = t4.twin_id AND t4.model_id IN ('dtmi:com:willowinc:Room;1','dtmi:com:willowinc:Level;1')
    LEFT JOIN transformed.twins_relationships_deduped tr5 ON t4.twin_id = tr5.source_twin_id and tr4.relationship_name IN ('locatedIn','isPartOf')
    LEFT JOIN transformed.levels_buildings lb ON lb.id = COALESCE(tr5.target_twin_id,tr3.target_twin_id)
    LEFT JOIN transformed.levels_buildings lb2 on ca.floor_id = lb2.floor_id
    WHERE 
                o.all_extends ilike '%HVACEquipment%'
            AND ca.model_id IN (
                    'dtmi:com:willowinc:SetpointOffset;1',
                    'dtmi:com:willowinc:Offset;1',
                    'dtmi:com:willowinc:ZoneAirTemperatureSensor;1',
                    'dtmi:com:willowinc:AirTemperatureSetpoint;1',
                    'dtmi:com:willowinc:CoolingZoneAirTemperatureSetpoint;1',
                    'dtmi:com:willowinc:HeatingZoneAirTemperatureSetpoint;1',
                    'dtmi:com:willowinc:EffectiveCoolingZoneAirTemperatureSetpoint;1',
                    'dtmi:com:willowinc:EffectiveHeatingZoneAirTemperatureSetpoint;1',
                    'dtmi:com:willowinc:EffectiveZoneAirTemperatureSetpoint;1',
                    'dtmi:com:willowinc:TargetZoneAirTemperatureSetpoint;1',
                    'dtmi:com:willowinc:OccupancySensor;1',
                    'dtmi:com:willowinc:OccupiedState;1',
                    'dtmi:com:willowinc:OccupancySetpoint;1',
                    'dtmi:com:willowinc:OccupiedActuator;1',
                    'dtmi:com:willowinc:OccupiedCoolingSetpoint;1',
                    'dtmi:com:willowinc:OccupiedCoolingZoneAirTemperatureSetpoint;1',
                    'dtmi:com:willowinc:OccupiedHeatingSetpoint;1',
                    'dtmi:com:willowinc:OccupiedHeatingZoneAirTemperatureSetpoint;1',
                    'dtmi:com:willowinc:UnoccupiedCoolingSetpoint;1',
                    'dtmi:com:willowinc:UnoccupiedCoolingZoneAirTemperatureSetpoint;1',
                    'dtmi:com:willowinc:UnoccupiedHeatingSetpoint;1',
                    'dtmi:com:willowinc:UnoccupiedHeatingZoneAirTemperatureSetpoint;1',
                    'dtmi:com:willowinc:ZoneAirTemperatureSetpoint;1',
                    'dtmi:com:willowinc:TargetZoneAirTemperatureSetpoint;1'
                    )
        AND IFNULL(o.deleted,false) = false
        AND IFNULL(t.is_deleted,false) = false
        AND IFNULL(t3.is_deleted,false) = false
        AND IFNULL(t4.is_deleted,false) = false
        AND IFNULL(tr.is_deleted,false) = false
        AND IFNULL(tr2.is_deleted,false) = false
        AND IFNULL(tr3.is_deleted,false) = false
        AND IFNULL(tr4.is_deleted,false) = false
        AND IFNULL(tr5.is_deleted,false) = false
QUALIFY ROW_NUMBER() OVER (PARTITION BY ca.asset_id, ca.trend_id,zone_name,room_name ORDER BY COALESCE(lb.level_name,t3.name,lb2.level_name)) = 1
;