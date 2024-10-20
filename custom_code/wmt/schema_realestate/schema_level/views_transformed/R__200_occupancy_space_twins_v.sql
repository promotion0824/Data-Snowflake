-- ******************************************************************************************************************************
-- Create view and persist as a table in transformed
-- ******************************************************************************************************************************CREATE OR REPLACE VIEW transformed.space_occupancy_assets AS 
CREATE OR REPLACE VIEW transformed.occupancy_space_twins_v AS
SELECT DISTINCT
    ca.building_id,
    ca.building_name,
    ca.site_id,
    ca.model_id,
    ca.trend_id,
    NULLIF(ca.external_id,'') AS external_id,
    ca.capability_id,
    ca.model_id_asset,
    ca.asset_id,
    ca.asset_name,
    CASE WHEN t7.model_id LIKE 'dtmi:com:willowinc:%Equipment;1' THEN t7.name ELSE ca.asset_name END AS equipment_name,
    CASE WHEN ca.model_id_asset = 'dtmi:com:willowinc:OccupancyZone;1' THEN ca.asset_name
         WHEN t3.model_id = 'dtmi:com:willowinc:OccupancyZone;1' THEN t3.name
         WHEN t5.model_id = 'dtmi:com:willowinc:OccupancyZone;1' THEN t5.name 
         ELSE NULL 
    END AS occupancy_zone,
    COALESCE(SPLIT_PART(REPLACE(t5.model_id, ';',':'),':',4),'Room') AS zone_type,
    CASE WHEN t2.model_id = 'dtmi:com:willowinc:Room;1' THEN t2.name 
         WHEN zone_type = 'Room' THEN occupancy_zone
         ELSE NULL 
    END AS room,
    CASE WHEN t5.model_id = 'dtmi:com:willowinc:Workstation;1' THEN t5.name ELSE NULL END AS workstation_name,
    t5.raw_json_value:customProperties.externalIds.NuvoloLocationSysId::STRING AS nuvolo_location_id,
    COALESCE(ca.asset_detail:customProperties.capacity.maxOccupancy::STRING,t3.raw_json_value:customProperties.capacity.maxOccupancy::STRING) AS max_occupancy, 
    COALESCE(ca.asset_detail:customProperties.capacity.seatingCapacity::STRING,t3.raw_json_value:customProperties.capacity.seatingCapacity::STRING)  AS seating_capacity,
    COALESCE(lb.id,t2.twin_id) as level_id, 
    COALESCE(lb.level_name, lb2.level_name) AS level_name,
    COALESCE(lb.floor_sort_order,lb2.floor_sort_order) AS floor_sort_order,
    t6.name AS manufacturer,
    ca.time_zone
FROM transformed.capabilities_assets ca
-- Asset located in Room
LEFT JOIN transformed.twins_relationships_deduped tr2 ON ca.asset_id = tr2.source_twin_id AND tr2.relationship_name IN ('locatedIn','isPartOf')
LEFT JOIN transformed.twins t2                        ON tr2.target_twin_id = t2.twin_id 
-- Asset serves OccupancyZone
LEFT JOIN transformed.twins_relationships_deduped tr3 ON ca.asset_id = tr3.target_twin_id AND tr3.relationship_name = 'servedBy'
LEFT JOIN transformed.twins t3                        ON tr3.source_twin_id = t3.twin_id  AND t3.model_id = 'dtmi:com:willowinc:OccupancyZone;1'
-- OccupancyZone isPartOf Room
LEFT JOIN transformed.twins_relationships_deduped tr4 ON t3.twin_id = tr4.source_twin_id  AND tr4.relationship_name IN ('locatedIn','isPartOf')
--LEFT JOIN transformed.twins t4                        ON tr4.target_twin_id = t4.twin_id  AND t4.model_id IN ('dtmi:com:willowinc:Room;1')
-- ZoneType derived from locatedIn 
LEFT JOIN transformed.twins_relationships_deduped tr5 on t3.twin_id = tr5.target_twin_id AND tr5.relationship_name IN ('locatedIn','isPartOf')
LEFT JOIN transformed.twins t5 on tr5.source_twin_id = t5.twin_id
-- OccupancyZone isPartOf Level
LEFT JOIN transformed.levels_buildings lb             ON tr4.target_twin_id = lb.id
LEFT JOIN transformed.levels_buildings lb2            ON t2.twin_id = lb2.id
-- Equipment servedBy OccupancyZone
LEFT JOIN transformed.twins_relationships_deduped tr7 ON ca.asset_id = tr7.source_twin_id AND tr7.relationship_name = 'servedBy'
LEFT JOIN transformed.twins t7                        ON tr7.target_twin_id = t7.twin_id  AND t7.model_id LIKE 'dtmi:com:willowinc:%Equipment;1'
-- Manufactured By
LEFT JOIN transformed.twins_relationships_deduped tr6 ON ca.asset_id = tr6.source_twin_id AND tr6.relationship_name = ('manufacturedBy')
LEFT JOIN transformed.twins t6                        ON tr6.target_twin_id = t6.twin_id AND t6.model_id = 'dtmi:com:willowinc:Company;1'
WHERE
    ca.model_id IN ('dtmi:com:willowinc:OccupancySensor;1', 'dtmi:com:willowinc:PeopleCountSensor;1')
AND occupancy_zone IS NOT NULL 
AND (t2.model_id NOT IN ('dtmi:com:willowinc:OccupancyZone;1') OR t2.model_id IS NULL)
AND (t5.model_id NOT IN ('dtmi:com:willowinc:OccupancySensorEquipment;1') OR t5.model_id IS NULL)
AND ifnull(tr2.is_deleted,false) = false
AND ifnull( t2.is_deleted,false) = false
AND ifnull(tr3.is_deleted,false) = false
AND ifnull( t3.is_deleted,false) = false
AND ifnull(tr4.is_deleted,false) = false
AND ifnull(tr5.is_deleted,false) = false
AND ifnull(t5.is_deleted,false)  = false
QUALIFY ROW_NUMBER() OVER (PARTITION BY ca.trend_id, ca.external_id ORDER BY zone_type NULLS LAST) = 1
;

CREATE OR REPLACE TABLE transformed.occupancy_space_twins AS SELECT * FROM transformed.occupancy_space_twins_v;