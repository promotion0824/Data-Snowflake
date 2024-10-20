-- ******************************************************************************************************************************
-- Create views
-- This is for internal use for investigating twins/models/relationships
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW published.twins_models_summary AS
SELECT 
        t.site_id,
        s.name AS site_name,
        o.model_level_1 AS ontology_level_1,
        o.model_level_2 AS ontology_level_2,
        o.model_level_3 AS ontology_level_3,
		o.model_level_4 AS ontology_level_4,
        t.model_id AS model_id,
		t.name AS twin_name,
        t.twin_id AS twin_id,
        t.tags AS tags,
        t.raw_json_value AS twin_detail,
        tr.relationship_name,
        t2.model_id AS parent_asset_model,
        t2.name AS parent_asset_name,
        t2.twin_id AS parent_asset_id,
		-- If we add other relationship_name type, make this come from actual relationship
        Case When ca.id IS NOT NULL THEN 'isCapabilityOf' ELSE NULL END AS capability_relationship,
        ca.capability_name,
		ca.id AS capability_id,
		ca.model_id AS model_id_capability,
		ca.capability_type,
		ca.trend_id,
		ca.trend_interval,
		ca.unit,
		ca.tags AS tags_capability,
		o.path AS ontology_asset_path,
		o2.path AS ontology_capability_path
	FROM transformed.twins t
    LEFT JOIN transformed.directory_core_sites s 
        ON (t.site_id = s.site_id)
    LEFT JOIN transformed.capabilities_assets ca
        ON (t.twin_id = ca.asset_id)
    LEFT JOIN transformed.ontology_buildings o
        ON (t.model_id = o.id)
	LEFT JOIN transformed.ontology_buildings o2
		ON (ca.model_id = o2.id)
    LEFT JOIN transformed.twins_relationships tr ON t.twin_id = tr.source_twin_id
    LEFT JOIN transformed.twins t2 ON tr.target_twin_id = t2.twin_id
    WHERE (relationship_name NOT IN ('hasDocument') OR relationship_name IS NULL)
	  AND IFNULL(t.is_deleted,false) = false
 ;