-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.buildings AS
WITH cte_customer AS (
	SELECT TOP 1 customer_id,portfolio_id FROM transformed.directory_core_sites dc
)
	SELECT
		ts.twin_id AS building_id,
		ts.name AS building_name,
        ts.raw_json_value:customProperties.externalIds.Real_Estate_ID::STRING as LucernixId,
		ts.model_id,
		ts.raw_json_value:customProperties.timeZone.name::VARCHAR(100) AS time_zone,
        COALESCE(t2.name,T.name) as region,
        t.name AS campus,
		ts.raw_json_value:customProperties.area.grossArea::NUMBER(34, 0) AS gross_area,
		ts.raw_json_value:customProperties.area.grossAreaUnit::VARCHAR(50) AS gross_area_unit,
		ts.raw_json_value:customProperties.area.rentableArea::NUMBER(36, 2) AS rentable_area,
		ts.site_id,
		s.name as site_name,
		ts.raw_json_value:customProperties.type::VARCHAR(100) AS type,
		ts.raw_json_value:customProperties::VARIANT AS custom_properties,
		COALESCE(s.portfolio_id, c.portfolio_id) AS portfolio_id,
		COALESCE(s.customer_id, c.customer_id) AS customer_id,
		ts._stage_record_id,
		ts._loader_run_id,
		ts._ingested_at,
		ts._staged_at
	FROM transformed.twins ts
	LEFT JOIN transformed.sites s ON ts.site_id = s.site_id
	CROSS JOIN cte_customer c
    LEFT JOIN transformed.twins_relationships_deduped tr ON building_id = tr.source_twin_id
    LEFT JOIN transformed.twins t ON tr.target_twin_id = t.twin_id
    LEFT JOIN transformed.twins_relationships_deduped tr2 ON t.twin_id = tr2.source_twin_id AND tr2.relationship_name = 'isPartOf'
    LEFT JOIN transformed.twins t2 ON tr2.target_twin_id = COALESCE(t2.twin_id, building_id)
	WHERE ts.model_id IN ('dtmi:com:willowinc:Building;1','dtmi:com:willowinc:BuildingTower;1','dtmi:com:willowinc:Substructure;1')
		AND (tr.relationship_name = 'isPartOf' OR tr.relationship_name IS NULL)
		AND IFNULL(ts.is_deleted,false) = false
		AND  IFNULL(t.is_deleted,0) = 0
		AND  IFNULL(tr.is_deleted,0) = 0
		AND  IFNULL(t2.is_deleted,0) = 0
		AND  IFNULL(tr2.is_deleted,0) = 0
;