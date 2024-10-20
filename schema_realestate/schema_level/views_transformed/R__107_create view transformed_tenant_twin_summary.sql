-- ******************************************************************************************************************************
-- Create view for tenant spaces
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW transformed.tenant_twin_summary AS

	SELECT DISTINCT 
		s.name AS site_name,
		ts.site_id,
		ts.model_id,
		ts.name AS space_name,
		ts.raw_json_value:customProperties.area.grossArea AS gross_area_space,
		ts.floor_id,
		tr.source_twin_id,
		tr.relationship_name,
		tr.target_twin_id AS unit_id,
		tt.model_id AS unit_model_id,
		tt.name AS unit_name,
		tr3.target_twin_id AS leased_status,
		tt3.raw_json_value:customProperties.leaseStart::DATE AS lease_start,
		tt3.raw_json_value:customProperties.leaseEnd::DATE AS lease_end,
        tt4.twin_id AS leasee_id,
        tt4.name AS leasee_name,
        tt4.is_deleted AS leasee_is_deleted,
		l.level_name,
        l.level_number,
		NULLIF(l.building_detail:modelId::STRING,'dtmi:com:willowinc:Building;1') AS sub_building_model,
		CASE WHEN sub_building_model IS NULL THEN NULL ELSE l.building_id END AS sub_building_id,
		CASE WHEN sub_building_model IS NULL THEN NULL ELSE l.building_detail:customProperties.name::STRING END AS sub_building_name,
		tr2.relationship_name AS building_relationship,
		tr2.target_twin_id AS building_id,
        tt2.name AS building_name,
        tt2.model_id AS building_model_id
	FROM transformed.twins ts
		JOIN transformed.twins_relationships_deduped tr 
		  ON (ts.twin_id = tr.source_twin_id)
		JOIN transformed.twins tt 
		  ON (tr.target_twin_id = tt.twin_id)
		LEFT JOIN transformed.levels_buildings l 
		  ON (ts.floor_id = l.unique_id)
		-- building
		LEFT JOIN transformed.twins_relationships_deduped tr2 
			   ON (l.building_id = tr2.source_twin_id)
			  AND (tr2.relationship_name IN ('isPartOf','includedIn'))
		-- tenant unit
		JOIN transformed.twins tt2
		  ON (tr2.target_twin_id = tt2.twin_id)
		-- leased
		LEFT JOIN transformed.twins_relationships_deduped tr3
			   ON (tt.twin_id = tr3.source_twin_id)
			  AND (tr3.relationship_name = 'hasLease')
              AND (IFNULL(tr3.is_deleted,false) = false)
		LEFT JOIN transformed.twins tt3
		  ON (tr2.target_twin_id = tt2.twin_id)
	     AND (tt3.model_id = 'dtmi:com:willowinc:Lease;1')
		LEFT JOIN transformed.twins_relationships_deduped tr4
		  ON (tr4.source_twin_id = tt3.twin_id)
         AND (tr4.relationship_name = 'leasee')
         AND (IFNULL(tr4.is_deleted,false) = false)
		LEFT JOIN transformed.twins tt4
		  ON (tr4.target_twin_id = tt4.twin_id)
	     AND (tt4.model_id = 'dtmi:com:willowinc:Company;1')
        LEFT JOIN transformed.sites s
               ON (ts.site_id = s.site_id)
	WHERE tt.model_id = 'dtmi:com:willowinc:TenantUnit;1'
      AND IFNULL(ts.is_deleted,false) = false
      AND IFNULL(tt.is_deleted,false) = false
      AND IFNULL(tt2.is_deleted,false) = false

	UNION 

	SELECT 
		s.name AS site_name,
		ts.site_id,
		ts.model_id,
		ts.name AS space_name,
		ts.raw_json_value:customProperties.area.grossArea AS gross_area_space,
		ts.floor_id,
		tr.source_twin_id,
		tr.relationship_name,
		tr.target_twin_id AS common_space,
		tt.model_id,
		tt.name,
		'N/A' AS leased_status,
		NULL AS lease_start,
		NULL AS lease_end,
		NULL AS leasee_id,
		NULL AS leasee_name,
		NULL AS leasee_is_deleted,
		l.level_name,
        l.level_number,
		NULLIF(l.building_detail:modelId::string,'dtmi:com:willowinc:Building;1') AS sub_building_model,
		CASE WHEN sub_building_model IS NULL THEN NULL ELSE l.building_id END AS sub_building_id,
		CASE WHEN sub_building_model IS NULL THEN NULL ELSE l.building_detail:customProperties.name::string END AS sub_building_name,
		tr2.relationship_name AS building_relationship,
		tr2.target_twin_id AS building_id,
        tt2.name as building_name,
        tt2.model_id as building_model_id
	FROM transformed.twins ts
		JOIN transformed.twins_relationships_deduped tr 
		  ON (ts.twin_id = tr.source_twin_id)
		JOIN transformed.twins tt 
		  ON (tr.target_twin_id = tt.twin_id)
		LEFT JOIN transformed.levels_buildings l 
			   ON (ts.floor_id = l.unique_id)
		LEFT JOIN transformed.twins_relationships_deduped tr2 
			   ON (l.building_id = tr2.source_twin_id)
			  AND (tr2.relationship_name IN ('isPartOf','includedIn'))
		JOIN transformed.twins tt2
		  ON (tr2.target_twin_id = tt2.twin_id)
        LEFT JOIN transformed.sites s
               ON (ts.site_id = s.site_id)
	WHERE tt.model_id = 'dtmi:com:willowinc:BuildingCommonArea;1'
	  AND IFNULL(ts.is_deleted,false) = false
      AND IFNULL(tt.is_deleted,false) = false
      AND IFNULL(tt2.is_deleted,false) = false
;

CREATE OR REPLACE VIEW published.tenant_twin_summary AS
SELECT * FROM transformed.tenant_twin_summary;