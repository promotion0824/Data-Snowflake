-- ------------------------------------------------------------------------------------------------------------------------------
-- create View
-- ------------------------------------------------------------------------------------------------------------------------------CREATE OR REPLACE VIEW published.occupancy_space_opening_twins AS 
CREATE OR REPLACE VIEW published.occupancy_space_opening_twins AS 
SELECT 
    building_id,
    building_name,
    site_id,
    trend_id,
    model_id,
    external_id,
    capability_id,
    capability_name,
    space_opening_type,
    space_opening_id,
    space_opening_name,
    time_zone
FROM transformed.occupancy_space_opening_twins 
;