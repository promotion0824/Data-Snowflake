-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW transformed.spaces_levels_v AS
	SELECT
		r.source_twin_id AS id,
		ts.name AS space_name,
		ts.model_id,
		ts.site_id,
		TRY_TO_DECIMAL(tt.raw_json_value:customProperties.seatingCapacity::STRING,34, 0) AS space_capacity,
		TRY_TO_DECIMAL(ts.raw_json_value:customProperties.area.usableArea::STRING,36, 2) AS usable_area_space,
		TRY_TO_DECIMAL(ts.raw_json_value:customProperties.maxOccupancy::STRING,34, 0) AS max_occupancy,
		TRY_TO_DECIMAL(ts.raw_json_value:customProperties.capacity.seatingCapacity::STRING,34, 0) AS seating_capacity,
		CASE WHEN ts.model_id = 'dtmi:com:willowinc:Level;1' THEN ts.twin_id ELSE tt.twin_id END AS level_id,
		CASE WHEN ts.model_id = 'dtmi:com:willowinc:Level;1' THEN ts.name ELSE tt.name END AS level_name,
		COALESCE(ts.floor_id, tt.unique_id, ts.unique_id) AS floor_id,
		TRY_TO_DECIMAL(tt.raw_json_value:customProperties.seatingCapacity::STRING,34, 0) AS level_capacity,
		TRY_TO_DECIMAL(tt.raw_json_value:customProperties.area.usableArea::STRING,36, 2) AS usable_area_level,
		COALESCE(sc_floors.sort_order, scf2.sort_order) AS floor_sort_order,
		ts._stage_record_id,
		ts._loader_run_id,
		ts._ingested_at,
		ts._staged_at
	FROM transformed.twins ts
		LEFT JOIN transformed.twins_relationships_deduped r 
			ON (ts.twin_id = r.source_twin_id)
			AND r.relationship_name IN ('locatedIn','isPartOf','serves')
		LEFT JOIN transformed.twins tt 
			ON (tt.twin_id = r.target_twin_id)
           AND (tt.model_id IN ('dtmi:com:willowinc:Room;1','dtmi:com:willowinc:Level;1')
            OR tt.model_id like 'dtmi:com:willowinc:%Zone%;1')
		LEFT JOIN transformed.site_core_floors sc_floors
		       ON (tt.site_id = sc_floors.site_id)
		      AND (tt.floor_id = sc_floors.id)
		LEFT JOIN transformed.site_core_floors scf2
		       ON (ts.site_id = scf2.site_id)
		      AND (ts.floor_id = scf2.id)
	WHERE 
	        (ts.model_id IN ('dtmi:com:willowinc:Room;1','dtmi:com:willowinc:Level;1')
         OR ts.model_id like 'dtmi:com:willowinc:%Zone%;1')
		AND IFNULL(r.is_deleted,false) = false
		AND IFNULL(ts.is_deleted,false) = false
		AND IFNULL(tt.is_deleted,false) = false
QUALIFY ROW_NUMBER() OVER (PARTITION BY ts.twin_id ORDER BY CASE WHEN tt.model_id = 'dtmi:com:willowinc:Level;1' THEN 0 ELSE 1 END DESC) = 1
;

CREATE OR REPLACE TABLE transformed.spaces_levels AS SELECT * FROM transformed.spaces_levels_v;