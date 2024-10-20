-- ******************************************************************************************************************************
-- Create view and persist as a table in transformed
-- ******************************************************************************************************************************CREATE OR REPLACE VIEW transformed.space_occupancy_assets AS 
CREATE OR REPLACE VIEW published.occupancy_space_twins AS
SELECT 
    building_id,
    building_name,
    site_id,
    model_id,
    trend_id,
    external_id,
    capability_id,
    model_id_asset,
    asset_id,
    asset_name,
    room,
    occupancy_zone,
    zone_type,
    max_occupancy, 
    seating_capacity,
    level_id,
    level_name,
    floor_sort_order,
    time_zone
FROM transformed.occupancy_space_twins
;