-- ------------------------------------------------------------------------------------------------------------------------------
-- create view for assets 
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW transformed.vergesense_assets_v AS
SELECT DISTINCT
    ca.model_id,
    ca.id AS capability_id,
    ca.capability_name,
    ca.asset_id,
    ca.asset_name,
    ca.model_id_asset, 
    a.space_id,
    a.space_name,
    a.model_id_space AS space_type,
    a.space_properties:customProperties.Room_Info.Seats::INTEGER AS seating_capacity,
    tnt.tenant_name,
    tnt.tenant_id,
    tnt.tenant_unit_id,
    tnt.tenant_unit_name,
    a.floor_id,
    l.id AS level_id,
    l.level_name,
    l.floor_sort_order,
    ca.building_id,
    ca.building_name,
    ca.trend_id,
    ca.site_id,
    ca.site_name,
    ca.time_zone
FROM transformed.capabilities_assets ca
LEFT JOIN transformed.spaces_levels s 
       ON (ca.asset_id = s.id)
LEFT JOIN transformed.assets_space a
       ON (ca.asset_id = a.asset_id)
LEFT JOIN transformed.tenant_served_by_twin tnt
       ON (a.space_id = tnt.asset_id)
LEFT JOIN transformed.levels_buildings l
       ON (a.floor_id = l.floor_id)
WHERE ca.model_id in (
    'dtmi:com:willowinc:PeopleCountSensor;1',
    'dtmi:com:willowinc:PeopleOccupancySensor;1',
    'dtmi:com:willowinc:OccupancySensor;1',
    'dtmi:com:willowinc:OccupiedState;1')
;

CREATE OR REPLACE TABLE transformed.vergesense_assets AS SELECT * FROM transformed.vergesense_assets_v;