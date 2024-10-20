-- ******************************************************************************************************************************
-- Create view - assets (including other space assets) to space
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW transformed.assets_space AS
    WITH cte_servedBy AS (
	SELECT DISTINCT
		ts.twin_id AS asset_id,
		ts.model_id AS model_id_asset,
        r.relationship_name,
		r.target_twin_id AS space_id,
        tt.name AS space_name,
		tt.model_id AS model_id_space,
		COALESCE(tt.floor_id, ts.floor_id, CASE WHEN tt.model_id = 'dtmi:com:willowinc:Level;1' THEN tt.unique_id ELSE NULL END) AS floor_id,
        tt.site_id,
		tt.raw_json_value:customProperties::VARIANT AS space_properties
	FROM transformed.twins ts
		 LEFT JOIN transformed.twins_relationships_deduped r ON (ts.twin_id = r.target_twin_id)
		 LEFT JOIN transformed.twins tt ON (tt.twin_id = r.source_twin_id)
	WHERE 
		r.relationship_name IN ('servedBy')
         AND (tt.model_id IN ('dtmi:com:willowinc:Room;1','dtmi:com:willowinc:Level;1')
          OR tt.model_id ILIKE 'dtmi:com:willowinc:%Zone%;1')
		AND IFNULL(tt.is_deleted,false) = false
        AND IFNULL(ts.is_deleted,false) = false
        AND IFNULL(r.is_deleted,false) = false
)
	SELECT DISTINCT
		r.source_twin_id AS asset_id,
		ts.model_id AS model_id_asset,
        r.relationship_name,
		r.target_twin_id AS space_id,
		tt.name AS space_name,
		tt.model_id AS model_id_space,
		COALESCE(ts.floor_id, tt.floor_id, CASE WHEN tt.model_id = 'dtmi:com:willowinc:Level;1' THEN tt.unique_id ELSE NULL END) AS floor_id,
        ts.site_id,
		tt.raw_json_value:customProperties::VARIANT AS space_properties
	FROM transformed.twins ts
		JOIN transformed.twins_relationships_deduped r 
		  ON (ts.twin_id = r.source_twin_id)
		JOIN transformed.twins tt 
		  ON (tt.twin_id = r.target_twin_id)
	WHERE NOT EXISTS (Select 1 FROM cte_servedBy cte WHERE ts.twin_id = cte.asset_id)
		AND r.relationship_name IN ('locatedIn','isPartOf')
        AND (tt.model_id IN ('dtmi:com:willowinc:Room;1','dtmi:com:willowinc:Level;1')
         OR tt.model_id ILIKE 'dtmi:com:willowinc:%Zone%;1')
		AND IFNULL(r.is_deleted,FALSE) = FALSE
		AND IFNULL(ts.is_deleted,FALSE) = FALSE
		AND IFNULL(tt.is_deleted,FALSE) = FALSE

    UNION ALL

	SELECT
		asset_id,
		model_id_asset,
        relationship_name,
		space_id,
        space_name,
		model_id_space,
		floor_id,
        site_id,
		space_properties
    FROM cte_servedBy
;