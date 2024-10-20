-- ------------------------------------------------------------------------------------------------------------------------------
-- create View
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW transformed.facit_trend_ids_v AS
SELECT
 c.building_id,
 c.building_name,
 c.site_id,
 c.site_name,
 c.time_zone,
 c.trend_id,
 c.id AS capability_id,
 c.model_id,
 c.capability_name,
 c.description AS capability_description,
 c.asset_id,
 c.asset_detail:displayName::string AS asset_name,
 split_part(replace(c.model_id_asset, ';',':'),':',4) AS category_name,
 l.id AS level_id,
 l.level_name,
 l.level_name AS floor_name,
 l.level_number,
 l.level_code AS floor_code,
 l.floor_sort_order,
 sl.space_name,
 sl.space_capacity,
 sl.seating_capacity,
 l.custom_properties:capacity.maxOccupancy ::STRING AS max_occupancy
FROM transformed.capabilities_assets c
    JOIN transformed.twins_relationships_deduped r 
      ON (c.id = r.source_twin_id)
    JOIN transformed.twins tt 
      ON (tt.twin_id = r.target_twin_id)
	LEFT JOIN transformed.spaces_levels sl 
		   ON (c.asset_id = sl.id)
	LEFT JOIN transformed.levels_buildings l 
		   ON (sl.level_id = l.id)
  LEFT JOIN transformed.sites s
       ON (c.site_id = s.site_id)
	WHERE (c.model_id in ('dtmi:com:willowinc:TotalEnteringPeopleCountSensor;1','dtmi:com:willowinc:TotalLeavingPeopleCountSensor;1') 
     OR (c.model_id in ('dtmi:com:willowinc:PeopleCountSensor;1') AND NOT c.capability_name ILIKE ANY ('Total People Count*','Unique People Count*','Net People Count'))) --These ones are CCure
    AND r.relationship_name = 'isCapabilityOf'
    AND tt.model_id in (
    'dtmi:com:willowinc:OccupancyZone;1',
    'dtmi:com:willowinc:Building;1'
    )
    AND IFNULL(r.is_deleted,false) = false
;
CREATE OR REPLACE TABLE transformed.facit_trend_ids AS SELECT * FROM transformed.facit_trend_ids_v;
