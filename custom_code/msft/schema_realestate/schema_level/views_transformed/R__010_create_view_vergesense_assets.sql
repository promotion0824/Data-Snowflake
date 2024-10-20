-- ------------------------------------------------------------------------------------------------------------------------------
-- create view for assets 
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW transformed.vergesense_assets AS
SELECT DISTINCT
    ca.model_id,
    ca.id AS capability_id,
    ca.capability_name,
    ca.asset_id,
    ca.asset_name,
    ca.model_id_asset,
    t.raw_json_value:customProperties.type::STRING AS space_type,
    t.raw_json_value:customProperties.capacity.seatingCapacity::INTEGER AS seating_capacity,
    s.id AS space_id,
    s.space_name,
	s.max_occupancy,
    s.usable_area_space AS usable_area,
    ca.floor_id,
	l.id AS level_id,
	l.level_name,
	l.level_capacity,
	l.usable_area_level,
	l.level_code AS floor_code,
	l.floor_sort_order,
    ca.trend_id,
    ca.building_id,
    ca.building_name,
    ca.site_id,
    ca.site_name,
	t.raw_json_value,
    ca.time_zone
FROM transformed.capabilities_assets ca
JOIN transformed.twins t ON ca.asset_id = t.twin_id AND IFNULL(t.is_deleted,FALSE) = FALSE
LEFT JOIN transformed.spaces_levels s 
       ON (ca.asset_id = s.id)
LEFT JOIN transformed.levels_buildings l
       ON (ca.floor_id = l.floor_id)
WHERE ca.model_id in (
    'dtmi:com:willowinc:PeopleCountSensor;1',
    'dtmi:com:willowinc:InferredOccupancySensor;1',
    'dtmi:com:willowinc:OccupancySensor;1',
    'dtmi:com:willowinc:OccupiedState;1'
);
