-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW transformed.capabilities_zones_v AS
	SELECT
		r.source_twin_id AS id,
		ts.name AS capability_name,
		LOWER(ts.trend_id) AS trend_id,
		ts.model_id,
		ts.unique_id,
		ts.external_id,
		ts.raw_json_value:customProperties.unit::VARCHAR(100) AS unit,
		ts.tags,
		ts.raw_json_value:customProperties.enabled::INTEGER AS enabled,
		ts.raw_json_value:customProperties.type::VARCHAR(100) AS capability_type,
		ts.raw_json_value:customProperties.description::VARCHAR(1000) AS description,
		ts.site_id,
		r.target_twin_id AS zone_id,
		ts.raw_json_value:customProperties.code::VARCHAR(100) AS zone_code,
		tt.name AS zone_name,
		tt.raw_json_value AS zone_detail,
		ts.raw_json_value:customProperties::VARIANT AS custom_properties,
		true AS is_active,
		ts._stage_record_id,
		ts._loader_run_id,
		ts._ingested_at,
		ts._staged_at
	FROM transformed.twins ts
		JOIN transformed.ontology_buildings os 
		  ON (ts.model_id = os.id)
		JOIN transformed.twins_relationships_deduped r 
		  ON (ts.twin_id = r.source_twin_id)
		JOIN transformed.twins tt 
		  ON (tt.twin_id = r.target_twin_id)
	WHERE os.model_level_1 = 'Capability'
		AND r.relationship_name IN ('isCapabilityOf','includedIn','locatedIn','isPartOf')
		AND tt.model_id ILIKE '%zone%' 
		AND IFNULL(r.is_deleted,false) = false
		AND IFNULL(ts.is_deleted,false) = false
		AND IFNULL(tt.is_deleted,false) = false
;

CREATE OR REPLACE TABLE transformed.capabilities_zones AS SELECT * FROM transformed.capabilities_zones_v;