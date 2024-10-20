-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW transformed.hvac_equipment_v AS
	SELECT
		r.source_twin_id AS id,
		ts.name AS equipment_name,
		ts.model_id,
		ts.unique_id,
		ts.external_id,
		ts.raw_json_value:customProperties.type::VARCHAR(100) AS type,
		ts.raw_json_value:customProperties.description::VARCHAR(1000) AS description,
		ts.site_id,
		LOWER(ts.trend_id) AS trend_id,
		ts.raw_json_value:customProperties.trendInterval::INTEGER AS trend_interval,
		ts.raw_json_value:customProperties.unit::VARCHAR(100) AS unit,
		r.target_twin_id AS space_id,
		tt.raw_json_value AS space_detail,
		ts.tags,
		tt.raw_json_value:modelId::VARCHAR(100) AS model_id_space,
		ts.raw_json_value:customProperties::VARIANT AS custom_properties,
		true AS is_active,
		ts._stage_record_id,
		ts._loader_run_id,
		ts._ingested_at,
		ts._staged_at
	FROM transformed.twins ts
		JOIN transformed.twins_relationships_deduped r 
			ON (ts.twin_id = r.source_twin_id)
		JOIN transformed.twins tt 
			ON (tt.twin_id = r.target_twin_id)
		JOIN transformed.ontology_buildings ot 
			ON (tt.model_id = ot.id)
	WHERE ts.model_id ILIKE ANY (
		'%HVACEquipment%',
		'%SensorEquipment%',
		'%AirHandlingUnit%',
		'%FanCoilUnit%',
		'%CAVBox%',
		'%ExhaustFan%',
		'%SupplyFan%',
		'%VAVBox%'
		)
		AND r.relationship_name IN ('locatedIn','isPartOf','serves', 'feeds')
		AND ot.model_level_1 = 'Space'  
		AND IFNULL(r.is_deleted,false) = false
		AND IFNULL(ts.is_deleted,false) = false
		AND IFNULL(tt.is_deleted,false) = false
;

CREATE OR REPLACE TABLE transformed.hvac_equipment AS SELECT * FROM transformed.hvac_equipment_v;