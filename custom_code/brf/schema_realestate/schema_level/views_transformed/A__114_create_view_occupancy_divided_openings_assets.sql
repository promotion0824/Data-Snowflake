-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.occupancy_divided_openings_assets_v AS
SELECT
    ca.model_id,
    ca.capability_name,
    ca.capability_id,
    ca.external_id,
    ca.trend_id,
    ca.model_id_asset,
    ca.asset_id,
    t2.Name AS entrance_name,
    tr2.relationship_name,
    tr2.Target_Twin_Id AS occupancy_zone,
    t3.model_id AS space_type,
    ca.building_id,
    ca.building_name,
    ca.site_id,
    ca.site_name
FROM transformed.capabilities_assets ca
JOIN transformed.twins_relationships_deduped tr1 ON ca.asset_id = tr1.Target_Twin_Id
JOIN transformed.twins t2 ON tr1.Source_Twin_Id = t2.twin_id
JOIN transformed.twins_relationships_deduped tr2 ON t2.twin_id = tr2.Source_Twin_Id
JOIN transformed.twins t3 ON tr2.Target_Twin_Id = t3.twin_id
WHERE ca.model_id_asset IN ('dtmi:com:willowinc:NonphysicalSpaceOpening;1')
  AND tr1.relationship_name = 'isPartOf'
  AND tr2.relationship_name = 'isEntryTo'
  AND ifnull(tr1.is_deleted,false) = false
  AND ifnull(t2.is_deleted,false)  = false
  AND ifnull(tr2.is_deleted,false) = false
  AND ifnull(t3.is_deleted,false)  = false
;

CREATE OR REPLACE TABLE transformed.occupancy_divided_openings_assets AS
SELECT * FROM transformed.occupancy_divided_openings_assets_v;
