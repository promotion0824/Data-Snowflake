-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW transformed.companies_v AS
	SELECT 
		ts.twin_id AS company_id,
		ts.name AS company_name,
		ts.model_id AS model_id,
		ts.raw_json_value:customProperties.code::VARCHAR(100) AS code,
		ts.unique_id,
		ts.site_id,
		ts.raw_json_value:customProperties::VARIANT AS custom_properties,
		true AS is_active,
		ts._stage_record_id,
		ts._loader_run_id,
		ts._ingested_at,
		ts._staged_at
	FROM transformed.twins ts
	WHERE ts.model_id ILIKE ('%Company%')
	AND IFNULL(ts.is_deleted,false) = false
;

CREATE OR REPLACE TABLE transformed.companies AS SELECT * FROM transformed.companies_v;