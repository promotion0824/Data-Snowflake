-- ------------------------------------------------------------------------------------------------------------------------------
-- create View
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW transformed.occupancy_building_twins AS
SELECT DISTINCT
    ca.capability_id,
    ca.capability_name,
    ca.trend_id,
    ca.model_id, 
    ca.model_id_asset,
    NULLIF(ca.external_id,'') AS external_id,
    ca.unit,
    ca.time_zone,
    ca.site_id, 
    t2.twin_id As building_id,
    t2.name AS building_name
FROM transformed.capabilities_assets ca
JOIN transformed.twins_relationships_deduped tr ON ca.asset_id = tr.source_twin_id AND tr.relationship_name = 'isEntryTo'
JOIN transformed.twins t2 ON tr.target_twin_id = t2.twin_id AND t2.model_id = 'dtmi:com:willowinc:Building;1'
WHERE ca.model_id = 'dtmi:com:willowinc:TotalEnteringPeopleCountSensor;1'
 AND IFNULL(t2.is_deleted,false) = false
 AND IFNULL(tr.is_deleted,false) = false
;