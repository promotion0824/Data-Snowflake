-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.buildings AS
	SELECT
		ts.twin_id AS building_id,
		ts.name AS building_name,
		ts.model_id,
		ts.raw_json_value:customProperties.timeZone.name::VARCHAR(100) AS time_zone,
		ts.unique_id,
		ts.raw_json_value:customProperties.area.grossArea::NUMBER(34, 0) AS gross_area,
		ts.raw_json_value:customProperties.area.grossAreaUnit::VARCHAR(50) AS gross_area_unit,
		ts.raw_json_value:customProperties.area.rentableArea::NUMBER(36, 2) AS rentable_area,
		ts.site_id,
		s.name as site_name,
		ts.raw_json_value:customProperties.type::VARCHAR(100) AS type,
		ts.raw_json_value:customProperties::VARIANT AS custom_properties,
		s.portfolio_id,
		s.customer_id,
		true AS is_active,
		ts._stage_record_id,
		ts._loader_run_id,
		ts._ingested_at,
		ts._staged_at
	FROM transformed.twins ts
	LEFT JOIN transformed.sites s ON ts.site_id = s.site_id
	WHERE ts.model_id IN ('dtmi:com:willowinc:Building;1','dtmi:com:willowinc:BuildingTower;1','dtmi:com:willowinc:Substructure;1')
		AND IFNULL(ts.is_deleted,false) = false
;