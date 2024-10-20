-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW published.twins_status AS
	SELECT 
		ts.unique_id,
		ts.twin_id AS id,
		ts.model_id,
		ts.name AS twin_name,
		ts.raw_json_value:customProperties.description::VARCHAR(1000) AS twin_description,
		ts.raw_json_value:customProperties.enabled::VARCHAR(100) AS is_enabled,
		ts.trend_id,
		ts.external_id,
		o.display_name AS category_name,
		dtdl.dtdl_rec_category,
		dtdl.description AS category_description,
		o.model_level_1,
		o.model_level_2,
		o.model_level_3,
		o.model_level_4,
		ts.site_id,
		s.name AS site_name,
		s.building_id,
		s.building_name,
        floors.level_name AS floor_name,
        floors.level_number,
	    floors.id AS level_twin_id,
        floors.floor_sort_order,
		s.time_zone,
		s.portfolio_id,
		s.customer_id,
		ts.is_deleted,
		ts.tags,
		ts.raw_json_value:customProperties::variant AS customProperties,
		ts.raw_json_value,
		ts.export_time
	FROM transformed.twins ts
		LEFT JOIN transformed.ontology_buildings o
			   ON (ts.model_id = o.id)
		LEFT JOIN transformed.sites s
			   ON (ts.site_id = s.site_id)
	LEFT JOIN transformed.levels_buildings floors
	  ON (ts.floor_id = floors.floor_id)
	LEFT JOIN transformed.dtdl_rec_categories dtdl
	  ON (o.model_level_1 = dtdl.category_name)
	WHERE IFNULL(ts.is_deleted,false) = FALSE
;