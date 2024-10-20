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
       ca.asset_detail:customProperties.capacity.seatingCapacity::INTEGER AS seating_capacity,
       ca.asset_detail:customProperties.capacity.maxOccupancy::INTEGER AS max_occupancy, 
       s.id AS space_id,
       a.space_name AS room,
       tnt.tenant_name,
       tnt.tenant_id,
       tnt.tenant_unit_id,
       tnt.tenant_unit_name,
       a.floor_id,
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
       ca.time_zone
FROM transformed.capabilities_assets ca
LEFT JOIN transformed.spaces_levels s 
       ON (ca.asset_id = s.id)
LEFT JOIN transformed.assets_space a
       ON (s.id = a.asset_id)
LEFT JOIN transformed.tenant_served_by_twin tnt
       ON (a.space_id = tnt.asset_id)
LEFT JOIN transformed.levels_buildings l
       ON (a.floor_id = l.floor_id)
WHERE ca.model_id in (
       'dtmi:com:willowinc:PeopleCountSensor;1',
       'dtmi:com:willowinc:InferredOccupancySensor;1',
       'dtmi:com:willowinc:OccupancySensor;1',
       'dtmi:com:willowinc:OccupiedState;1'
)
;