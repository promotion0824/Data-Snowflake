CREATE OR REPLACE VIEW transformed.occupancy_peoplecount_v AS
	SELECT
		ts.name AS sensor_name,
		ts.trend_id,
		ts.model_id AS model_id_capability,
		ts.raw_json_value:customProperties.type::VARCHAR(100) AS sensor_type,
		ts.site_id,
		r.source_twin_id AS sensor_id,
		tt.raw_json_value:id::VARCHAR(100) AS space_id,
		tt.raw_json_value:customProperties.name::VARCHAR(100) AS space_name,
		SPLIT_PART(REPLACE(tt.model_id, ';',':'),':',4) AS space_type,
		tt.raw_json_value:customProperties.capacity.seatingCapacity::NUMBER(34, 0) AS seating_capacity,
		tt.raw_json_value:customProperties.area.grossArea::NUMBER(36, 2) AS gross_area,
        tt.floor_id,
		l.level_name,
		l.id AS level_id,
		tt.raw_json_value
	FROM transformed.twins ts
		JOIN transformed.twins_relationships_deduped r 
			ON (ts.twin_id = r.source_twin_id)
		JOIN transformed.twins tt 
			ON (tt.twin_id = r.target_twin_id)
		LEFT JOIN transformed.spaces_levels s 
			ON (s.id = tt.raw_json_value:id::VARCHAR(100)) --space_id
		LEFT JOIN transformed.levels_buildings l 
			ON (tt.floor_id = l.floor_id)
	WHERE ts.model_id IN ('dtmi:com:willowinc:PeopleCountSensorEquipment;1', 'dtmi:com:willowinc:PeopleCountSensor;1',
                          'dtmi:com:willowinc:EnteringPeopleCountSensor;1', 'dtmi:com:willowinc:LeavingPeopleCountSensor;1')
		AND r.relationship_name IN ('isCapabilityOf','locatedIn','isPartOf', 'hostedBy')
		AND tt.model_id IN ('dtmi:com:willowinc:Room;1', 'dtmi:com:willowinc:Chair;1', 'dtmi:com:willowinc:Table;1', 'dtmi:com:willowinc:Door;1','dtmi:com:willowinc:OccupancyZone;1', 'dtmi:com:willowinc:Level;1', 'dtmi:com:willowinc:PeopleCountSensorEquipment;1')
		AND IFNULL(r.is_deleted,false) = false
		AND IFNULL(ts.is_deleted,false) = false
		AND IFNULL(tt.is_deleted,false) = false
;
CREATE OR REPLACE TABLE transformed.occupancy_peoplecount AS
	SELECT * FROM transformed.occupancy_peoplecount_v;

 
CREATE OR REPLACE VIEW transformed.occupancy_occupancysensor_v AS
	SELECT
		ts.name AS sensor_name,r.relationship_name ,
		ts.trend_id,
		ts.model_id AS model_id_capability,
		ts.raw_json_value:customProperties.type::VARCHAR(100) AS sensor_type,
		ts.site_id,
		r.source_twin_id AS sensor_id,
		tt.raw_json_value:id::VARCHAR(100) AS space_id,
		tt.raw_json_value:customProperties.name::VARCHAR(100) AS space_name,
		SPLIT_PART(REPLACE(tt.model_id, ';',':'),':',4) AS space_type,
		tt.raw_json_value:customProperties.capacity.seatingCapacity::NUMBER(34, 0) AS seating_capacity,
		tt.raw_json_value:customProperties.area.grossArea::NUMBER(36, 2) AS gross_area,
        tt.floor_id,
		l.level_name,
		l.id AS level_id,
		tt.raw_json_value
	FROM transformed.twins ts
		JOIN transformed.twins_relationships_deduped r 
			ON (ts.twin_id = r.source_twin_id)
		JOIN transformed.twins tt 
			ON (tt.twin_id = r.target_twin_id)
		LEFT JOIN transformed.spaces_levels s 
			ON (s.id = tt.raw_json_value:id::VARCHAR(100)) --space_id
		LEFT JOIN transformed.levels_buildings l 
			ON (tt.floor_id = l.floor_id)
	WHERE ts.model_id IN ('dtmi:com:willowinc:OccupancySensor;1','dtmi:com:willowinc:OccupiedState;1')
		AND r.relationship_name IN ('isCapabilityOf','locatedIn','isPartOf'
                                    --,'hostedBy'
                                   )
		AND tt.model_id IN ('dtmi:com:willowinc:Controller;1','dtmi:com:willowinc:Room;1', 'dtmi:com:willowinc:Chair;1', 'dtmi:com:willowinc:Table;1', 'dtmi:com:willowinc:Door;1','dtmi:com:willowinc:OccupancyZone;1', 'dtmi:com:willowinc:Level;1', 'dtmi:com:willowinc:PeopleCountSensorEquipment;1')
		AND IFNULL(r.is_deleted,false) = false
		AND IFNULL(ts.is_deleted,false) = false
		AND IFNULL(tt.is_deleted,false) = false
;
 CREATE OR REPLACE TABLE transformed.occupancy_occupancysensor AS
	SELECT * FROM transformed.occupancy_occupancysensor_v;