-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.capabilities_assets_v AS
	SELECT
		r.source_twin_id AS id,
		ts.twin_id AS capability_id,
		ts.name AS capability_name,
		LOWER(ts.trend_id) AS trend_id,
		ts.raw_json_value:customProperties.trendInterval::INTEGER AS trend_interval,
		ts.model_id,
		ts.unique_id,
		ts.external_id,
		ts.raw_json_value:customProperties.unit::VARCHAR(100) AS unit,
		ts.raw_json_value:customProperties.tags::VARIANT AS tags,
		ts.tags AS tags_string,
		ts.raw_json_value:customProperties.enabled::INTEGER AS enabled,
		ts.raw_json_value:customProperties.type::VARCHAR(100) AS capability_type,
		ts.raw_json_value:customProperties.description::VARCHAR(1000) AS description,
        tt.raw_json_value:customProperties.comments::VARCHAR(2000) AS comments,
		s.building_id,
		s.building_name,
		COALESCE(tt.site_id,ts.site_id) AS site_id,
		COALESCE(tt.site_dtid,ts.site_dtid) AS site_dtid,
		s.time_zone,
		s.name AS site_name,
		r.target_twin_id AS asset_id,
		tt.name AS asset_name,
		tt.raw_json_value AS asset_detail,
		tt.model_id AS model_id_asset,
		SPLIT_PART(REPLACE(tt.model_id, ';',':'),':',4) AS model_name_asset,
		COALESCE(tt.floor_id,ts.floor_id) AS floor_id,
		COALESCE(tt.floor_dtid, ts.floor_dtid) AS floor_dtid,
		ts.raw_json_value:customProperties::VARIANT AS custom_properties,
		model_level_1,
		model_level_2,
		model_level_3,
		model_level_4,
		model_level_5,
		ts.is_deleted source_twin_is_deleted,
        tt.is_deleted as target_twin_is_deleted,
        r.is_deleted as relationship_is_deleted,
		ts._stage_record_id,
		ts._loader_run_id,
		ts._ingested_at,
		ts._staged_at
	FROM transformed.twins ts
		LEFT JOIN transformed.ontology_buildings os 
		  ON (ts.model_id = os.id)
		JOIN transformed.twins_relationships_deduped r 
		  ON (ts.twin_id = r.source_twin_id)
		JOIN transformed.twins tt 
		  ON (tt.twin_id = r.target_twin_id)
		LEFT JOIN transformed.sites s 
		  ON (s.site_id = COALESCE(tt.site_id,ts.site_id))
	WHERE r.relationship_name IN ('isCapabilityOf') 
		AND IFNULL(r.is_deleted,FALSE) = FALSE
		AND IFNULL(ts.is_deleted,FALSE) = FALSE
		AND IFNULL(tt.is_deleted,FALSE) = FALSE;

CREATE OR REPLACE TRANSIENT TABLE transformed.capabilities_assets AS SELECT * FROM transformed.capabilities_assets_v;