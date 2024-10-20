-- ------------------------------------------------------------------------------------------------------------------------------
-- Create view
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.twins_relationships AS
	SELECT 
		ts.twin_id,
		ts.name AS twin_name,
		ts.raw_json_value:customProperties.description::VARCHAR(1000) AS twin_description,
		ts.raw_json_value:customProperties.enabled::BOOLEAN AS is_enabled,
		ts.tags,
		ts.model_id,
		o.display_name AS category_name,
		dtdl.dtdl_rec_category,
		dtdl.description AS category_description,
		o.model_level_1,
		o.model_level_2,
		o.model_level_3,
		o.model_level_4,
		ts.raw_json_value:customProperties.comments::STRING AS comments,
		ts.raw_json_value:customProperties.installationDate::DATE AS installation_date,
		ts.raw_json_value:customProperties.modelNumber::VARCHAR(1000) AS model_number,
		ts.raw_json_value:customProperties.serialNumber::VARCHAR(1000) AS serial_number,
		s.building_id,
		s.building_name,
		ts.site_id,
		s.name AS site_name,
		s.time_zone,
		s.portfolio_id,
		s.customer_id,
		ts.raw_json_value,
		r.relationship_name AS relationship_type,
		tt.model_id AS relationship_model_id,
		r.target_twin_id AS relationship_twin_id,
		r.relationship_id,
		ts.is_deleted AS twin_is_deleted,
		r.is_deleted AS relationship_is_deleted,
		tt.is_deleted AS target_twin_is_deleted,
		r.raw_json_value AS relationship_details,
		GREATEST(IFNULL(ts.export_time,'2018-01-01'), IFNULL(r.export_time,'2018-01-01'), IFNULL(tt.export_time,'2018-01-01'), ts._last_updated_at) AS export_time
	FROM transformed.twins ts
		LEFT JOIN transformed.twins_relationships_deduped r 
			   ON (ts.twin_id = r.source_twin_id)
		LEFT JOIN transformed.twins tt 
			   ON (r.target_twin_id = tt.twin_id)
		LEFT JOIN transformed.ontology_buildings o
			   ON (ts.model_id = o.id)
		LEFT JOIN transformed.sites s
			   ON (ts.site_id = s.site_id)
		LEFT JOIN transformed.dtdl_rec_categories dtdl
			   ON (o.model_level_1 = dtdl.category_name)
	WHERE 
		    IFNULL(ts.is_deleted,FALSE) = FALSE
		AND IFNULL(r.is_deleted,FALSE)  = FALSE
		AND IFNULL(tt.is_deleted,FALSE) = FALSE;