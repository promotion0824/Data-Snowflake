-- ------------------------------------------------------------------------------------------------------------------------------
-- create View
-- ------------------------------------------------------------------------------------------------------------------------------CREATE OR REPLACE VIEW published.occupancy_space_opening_twins AS 
CREATE OR REPLACE VIEW transformed.occupancy_space_opening_twins AS 
SELECT DISTINCT
    ca.building_id,
    ca.building_name,
    ca.site_id,
    ca.trend_id,
    ca.model_id,
    NULLIF(ca.external_id,'') AS external_id,
    ca.capability_id,
    ca.capability_name,
    SPLIT_PART(REPLACE(ca.model_id_asset, ';',':'),':',4) AS space_opening_type,
    ca.asset_id AS space_opening_id,
    ca.asset_name AS space_opening_name,
    ca.time_zone
FROM transformed.capabilities_assets ca
JOIN transformed.ontology_models o ON ca.model_id_asset = o.id
JOIN transformed.twins_relationships_deduped tr ON ca.asset_id = tr.source_twin_id AND tr.relationship_name IN ('isEntryTo','isExitFrom')
WHERE model_id IN ('dtmi:com:willowinc:TotalEnteringPeopleCountSensor;1','dtmi:com:willowinc:TotalLeavingPeopleCountSensor;1')
AND o.all_extends ilike '%SpaceOpening%'
AND IFNULL(tr.is_deleted,false) = false
;