-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW transformed.levels_buildings_v AS
	SELECT DISTINCT
		ts.twin_id AS id,
		ts.name AS level_name,
		ts.raw_json_value:customProperties.code::VARCHAR(100) AS level_code,
		regexp_substr(level_code, '[[:alpha:]]+') AS level_alpha,
		TRY_TO_NUMBER(REGEXP_REPLACE(level_code, '[a-z/-/A-z/./#/*]', '')) AS level_numeric,
		level_alpha || IFNULL((LPAD(level_numeric,2,0)),'') AS level_number,  --this should be parsed from twin properties
		COALESCE(sc_floors.sort_order,level_numeric) AS floor_sort_order,
		ts.model_id,
		ts.unique_id,
		ts.unique_id AS floor_id,
		ts.raw_json_value:customProperties.seatingCapacity::NUMBER(34, 0) AS level_capacity,
		ts.raw_json_value:customProperties.area.usableArea::NUMBER(36, 2) AS usable_area_level,
		ts.site_id,
		s.name AS site_name,
		ts.raw_json_value:customProperties.siteElevation::VARCHAR(100) AS site_elevation,
		ts.raw_json_value:customProperties.occupancy::VARIANT AS occupancy,
		r.target_twin_id AS building_id,
		tt.name AS building_name,
		tt.model_id AS building_model_id,
		tt.raw_json_value AS building_detail,
		ts.raw_json_value:customProperties::VARIANT AS custom_properties,
		true AS is_active,
		ts._stage_record_id,
		ts._loader_run_id,
		ts._ingested_at,
		ts._staged_at
	FROM transformed.twins ts
		LEFT JOIN transformed.twins_relationships r 
			   ON (ts.twin_id = r.source_twin_id)
               AND (IFNULL(r.is_deleted,false) = false)
		LEFT JOIN transformed.twins tt 
			   ON (r.target_twin_id = tt.twin_id)
              AND (IFNULL(tt.is_deleted,false) = false)
		LEFT JOIN transformed.site_core_floors sc_floors
		       ON (ts.site_id = sc_floors.site_id)
		      AND (ts.unique_id = sc_floors.id)
		LEFT JOIN transformed.sites s 
			   ON (ts.site_id = s.site_id)
	WHERE ts.model_id = 'dtmi:com:willowinc:Level;1'
		AND (r.relationship_name IN ('locatedIn','isPartOf') OR r.relationship_name IS NULL)
		-- AND (tt.model_id IN ('dtmi:com:willowinc:Building;1','dtmi:com:willowinc:BuildingTower;1','dtmi:com:willowinc:Substructure;1') OR tt.model_id IS NULL)
		AND IFNULL(ts.is_deleted,false) = false
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ts.unique_id ORDER BY ts.unique_id, ts._last_updated_at desc) = 1
;

CREATE OR REPLACE TABLE transformed.levels_buildings AS SELECT * FROM transformed.levels_buildings_v;