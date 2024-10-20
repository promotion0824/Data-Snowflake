-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW transformed.persons_v AS
	SELECT 
		tt.name AS person_id,
		ts.raw_json_value:customProperties.companyId::VARCHAR(255) AS company_id,
		tt.twin_id AS person_twin_id,
		ts.twin_id AS sensor_id,
		ts.name AS sensor_name,
		ts.raw_json_value:customProperties.type::VARCHAR(100) AS sensor_type,
		ts.model_id AS model_id_sensor,
		ts.external_id AS external_id_sensor,
		ts.unique_id,
		ts.trend_id,
		ts.site_id,
		ts.raw_json_value:customProperties::VARIANT AS custom_properties,
		true AS is_active,
		ts._stage_record_id,
		ts._loader_run_id,
		ts._ingested_at,
		ts._staged_at
	FROM transformed.twins ts
		JOIN transformed.twins_relationships r 
			ON (ts.twin_id = r.source_twin_id)
		JOIN transformed.twins tt 
			ON (tt.twin_id = r.target_twin_id)
	WHERE ts.model_id ILIKE ('%PeopleCountSensor%')
		AND r.relationship_name IN ('isCapabilityOf')
		AND ts.model_id ILIKE ('%person%')
		AND IFNULL(r.is_deleted,false) = false
		AND IFNULL(ts.is_deleted,false) = false
		AND IFNULL(tt.is_deleted,false) = false
;

CREATE OR REPLACE TABLE transformed.persons AS SELECT * FROM transformed.persons_v;