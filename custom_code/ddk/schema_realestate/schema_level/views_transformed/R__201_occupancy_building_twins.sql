-- ------------------------------------------------------------------------------------------------------------------------------
-- create View
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW transformed.occupancy_building_twins AS
SELECT 
    capability_id,
    capability_name,
    trend_id,
    model_id,
    NULLIF(external_id,'') AS external_id,
    unit,
    building_id,
    building_name,
    time_zone,
    site_id
FROM transformed.capabilities_assets ca
WHERE model_id_asset IN ('dtmi:com:willowinc:Building;1','dtmi:com:willowinc:OutdoorArea;1','dtmi:com:willowinc:Substructure;1');
