-- ------------------------------------------------------------------------------------------------------------------------------
-- create View
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW published.occupancy_building_twins AS
SELECT 
    capability_id,
    capability_name,
    trend_id,
    model_id,
    external_id,
    unit,
    building_id,
    building_name,
    time_zone,
    site_id
FROM transformed.occupancy_building_twins
;